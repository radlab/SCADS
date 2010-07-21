package edu.berkeley.cs.scads.storage

import java.util.concurrent.{BlockingQueue, ArrayBlockingQueue, ThreadPoolExecutor, TimeUnit}
import java.nio.ByteBuffer
import java.io.ByteArrayInputStream
import java.net.InetAddress

import scala.actors._
import scala.actors.Actor._
import scala.collection.mutable.ArrayBuffer

import org.apache.log4j.Logger
import com.sleepycat.je.{Cursor,Database, DatabaseConfig, DatabaseException, DatabaseEntry, Environment, LockMode, OperationStatus, Durability, Transaction}

import edu.berkeley.cs.scads.comm._
import edu.berkeley.cs.scads.comm.Conversions._

import org.apache.avro.generic.GenericData.{Array => AvroArray}
import org.apache.avro.Schema
import org.apache.avro.util.Utf8
import org.apache.avro.generic.{GenericDatumReader,GenericData}
import org.apache.avro.io.BinaryDecoder
import org.apache.avro.io.DecoderFactory

import org.apache.zookeeper.CreateMode

import com.googlecode.avro.runtime.ScalaSpecificRecord


class StorageHandler(env: Environment, root: ZooKeeperProxy#ZooKeeperNode) extends ServiceHandler {
  /* Hashmap and case class to hold relevent data about open namespaces */
  protected case class Namespace(db: Database, keySchema: Schema, comp: AvroComparator)
  protected var namespaces: Map[String, Namespace] = new scala.collection.immutable.HashMap[String, Namespace]

  /* Threadpool for execution of incoming requests */
  protected val outstandingRequests = new ArrayBlockingQueue[Runnable](1024)
  protected val executor = new ThreadPoolExecutor(5, 20, 30, TimeUnit.SECONDS, outstandingRequests)

  protected val logger = Logger.getLogger("scads.storagehandler")
  protected val remoteHandle = MessageHandler.registerService(this)

  /* Register a shutdown hook for proper cleanup */
  class SDRunner(sh: StorageHandler) extends Thread {
    override def run(): Unit = {
      sh.shutdown()
    }
  }
  java.lang.Runtime.getRuntime().addShutdownHook(new SDRunner(this))
  startup()

  private def startup(): Unit = {
    /* Register with the zookeper as an available server */
    val availServs = root.getOrCreate("availableServers")
    availServs.createChild(remoteHandle.toString, remoteHandle.toBytes, CreateMode.EPHEMERAL)

    /* TODO: Open already created namespaces? */
  }

  /* Implicit conversions */
  implicit def toOption[A](a: A): Option[A] = Option(a)
  implicit def toDbe(buff: Array[Byte]): DatabaseEntry = new DatabaseEntry(buff)
  implicit def toByteArray(dbe: DatabaseEntry): Array[Byte] = {
    if(dbe.getOffset == 0)
      return dbe.getData
    else
      throw new RuntimeException("Unimplemented")
  }

  private def decodeKey(ns:Namespace, dbe:DatabaseEntry): GenericData.Record = {
    val decoder = DecoderFactory.defaultFactory().createBinaryDecoder(dbe.getData(), dbe.getOffset(), dbe.getSize, null)
    val reader = new GenericDatumReader[GenericData.Record](ns.keySchema)
    reader.read(null,decoder)
  }

  private def mkSchema(old:Schema,newsize:Int):Schema = {
    val result = Schema.createRecord(old.getName, old.getDoc, old.getNamespace, old.isError);
    val fields = old.getFields
    val newfields = new java.util.ArrayList[Schema.Field](newsize-1)
    for (i <- (0 to (newsize-1))) {
      val of = fields.get(i)
      val nf = new Schema.Field(of.name,of.schema,of.doc,of.defaultValue,of.order)
      newfields.add(nf)
    }
    result.setFields(newfields)
    result
  }

  private class ShippedClassLoader(ba:Array[Byte],targetClass:String) extends ClassLoader {
    override def findClass(name:String):Class[_] = {
      if (name.equals(targetClass))
        defineClass(name, ba, 0, ba.size)
      else
        Class.forName(name)
    }
  }

  private def deserialize(name:String, ba:Array[Byte]):Any = {
    try {
      val loader = new ShippedClassLoader(ba,name)
      Class.forName(name,false,loader)
    } catch {
      case ex:java.io.IOException => {
        ex.printStackTrace
        (null,0)
      }
    }
  }

  class Request(src: RemoteNode, req: Message) extends Runnable {
    def reply(body: MessageBody) = {
      val resp = new Message
      resp.src = ActorName("Storage")
      resp.body = body
      if (req.src == null) {
        // TODO: do something here, since resp.dest cannot be null
      }
      resp.dest = req.src
      resp.id = req.id
      println("req: " + req + " resp: " + resp)
      MessageHandler.sendMessage(src, resp)
    }

    val process: PartialFunction[Object, Unit] = {

      case cr: ConfigureRequest => {
        openNamespace(cr.namespace, cr.partition)
        reply(EmptyResponse())
      }

      case gr: GetRequest => {
        val ns = namespaces(gr.namespace)
        val dbeKey: DatabaseEntry = new DatabaseEntry(gr.key)
        val dbeValue = new DatabaseEntry

        ns.db.get(null, dbeKey, dbeValue, LockMode.READ_COMMITTED)
        if (dbeValue.getData != null) {
          val retRec = new Record

          retRec.key = dbeKey
          retRec.value = toByteArray(dbeValue)

          reply(retRec)
        } else {
          reply(EmptyResponse())
        }
      }

      case pr: PutRequest => {
        val ns = namespaces(pr.namespace)
        val key: DatabaseEntry = pr.key
        val txn = env.beginTransaction(null, null)

	pr.value match {
	  case Some(v) => ns.db.put(txn, key, new DatabaseEntry(v))
	  case None => ns.db.delete(txn, key)
	}

        txn.commit(Durability.COMMIT_NO_SYNC)
        reply(EmptyResponse())
      }

      case tsr: TestSetRequest => {
        val ns = namespaces(tsr.namespace)
        val txn = env.beginTransaction(null, null)
        val dbeKey: DatabaseEntry = tsr.key
        val dbeEv = new DatabaseEntry()

        /* Set up partial get if the existing value specifies a prefix length */
        if(tsr.prefixMatch) {
          dbeEv.setPartial(true)
          dbeEv.setPartialLength(tsr.expectedValue.get.length)
        }

        /* Get the current value */
        ns.db.get(txn, dbeKey, dbeEv, LockMode.READ_COMMITTED)
	val expValue = tsr.expectedValue.getOrElse(null)

        if((dbeEv.getData == null && tsr.expectedValue.isDefined) ||
           (dbeEv.getData != null && tsr.expectedValue.isEmpty) ||
           (dbeEv.getData != null && tsr.expectedValue.isDefined && !dbeEv.equals(expValue))) {
             /* Throw exception if expected value doesnt match present value */
             txn.abort
             logger.warn("TSET FAILURE")
             val tsf = new TestAndSetFailure
             tsf.key = dbeKey
             tsf.currentValue = Some(dbeEv)
             reply(tsf)
           } else {
             /* Otherwise perform the put and commit */
             if(tsr.value == null)
               ns.db.delete(txn, dbeKey)
             else {
               val dbeValue: DatabaseEntry = tsr.value
               ns.db.put(txn, dbeKey, dbeValue)
             }
             txn.commit
             reply(EmptyResponse())
           }
      }

      case grr: GetRangeRequest => {
        val ns = namespaces(grr.namespace)
        val recordSet = new RecordSet
        val records = new ArrayBuffer[Record]
        iterateOverRange(ns, grr.range, false, (key, value, cursor) => {
          val rec = new Record
          rec.key = key.getData
          rec.value = value.getData
          records += rec
        })
        recordSet.records = records.toList
        reply(recordSet)
      }

      case gpr: GetPrefixRequest => {
        val ns = namespaces(gpr.namespace)

        val tschema = mkSchema(ns.keySchema,gpr.fields)
        val recordSet = new RecordSet
        val records = new ArrayBuffer[Record]

        var remaining = gpr.limit match {
          case None       => -1
          case Some(i) => i
        }
        var ascending = gpr.ascending

        val dbeKey:DatabaseEntry = gpr.start
        val dbeValue = new DatabaseEntry
        val cur = ns.db.openCursor(null,null)

        try {
          var stat = cur.getSearchKeyRange(dbeKey, dbeValue, null)

          // check return value of stat, since if we're looking for the last key desc
          // then stat will not succeed
          if (stat == OperationStatus.NOTFOUND) {
            stat = cur.getLast(dbeKey, dbeValue, null)
          }

          if (!ascending) {
            // skip backwards because we've probably gone too far forward
            while (stat == OperationStatus.SUCCESS &&
                   (org.apache.avro.io.BinaryData.compare(dbeKey.getData,dbeKey.getOffset,gpr.start,0,tschema) > 0)) {
                     stat = cur.getPrev(dbeKey,dbeValue,null)
                   }
          }

          while(stat == OperationStatus.SUCCESS &&
                remaining != 0 &&
                (org.apache.avro.io.BinaryData.compare(dbeKey.getData,dbeKey.getOffset,gpr.start,0,tschema) == 0))
          {
            val rec = new Record
            rec.key = dbeKey.getData
            rec.value = dbeValue.getData
            records += rec
            stat =
              if (gpr.ascending)
                cur.getNext(dbeKey, dbeValue, null)
              else
                cur.getPrev(dbeKey, dbeValue, null)
            remaining -= 1
          }
          recordSet.records = records.toList
          cur.close
        } catch {
          case t:Throwable => {
            cur.close
            throw t
          }
        }
        reply(recordSet)
      }

      case rrr: RemoveRangeRequest => {
        val ns = namespaces(rrr.namespace)
        iterateOverRange(ns, rrr.range, true, (key, value, cursor) => {
          cursor.delete()
        })
        reply(EmptyResponse())
      }

      case crr: CountRangeRequest => {
        val ns = namespaces(crr.namespace)
        var c = 0
        iterateOverRange(ns, crr.range, false, (key, value, cursor) => {
          c += 1
        })
        reply(CountRangeResponse(c))
      }

      case crr: CopyRangesRequest => throw new RuntimeException("Unimplemented")
      case csreq:CopyStartRequest => throw new RuntimeException("Unimplemented")
      case srr: SyncRangeRequest => throw new RuntimeException("Unimplemented")
      case ssreq:SyncStartRequest => throw new RuntimeException("Unimplemented")

      case fmreq: FlatMapRequest => throw new RuntimeException("Unimplemented")
      case filtreq:FilterRequest  => throw new RuntimeException("Unimplemented")

      case foldreq:FoldRequest  => throw new RuntimeException("Unimplemented")
      case foldreq:FoldRequest2L  => throw new RuntimeException("Unimplemented")
    }

    def run():Unit = {
      try {
        //println(req)
        process(req.body)
      }
      catch {
        case e: Throwable => {
          logger.error("ProcessingException", e)
          e.printStackTrace
          var cause = e.getCause
          while (cause != null) {
            e.printStackTrace
            cause = e.getCause
          }
          val resp = new ProcessingException
          resp.cause = e.toString()
          resp.stacktrace = e.getStackTrace().mkString("\n")
          reply(resp)
        }
      }
    }
  }

  private def iterateOverRange(ns: Namespace, range: KeyRange, needTXN: Boolean, func: (DatabaseEntry, DatabaseEntry, Cursor) => Unit): Unit =
    iterateOverRange(ns,range,needTXN,func,null)

  private def iterateOverRange(ns: Namespace, range: KeyRange, needTXN: Boolean, func: (DatabaseEntry, DatabaseEntry, Cursor) => Unit, finalFunc: (Cursor) => Unit): Unit = {
    logger.debug("entering iterateOverRange")
    val dbeKey = new DatabaseEntry()
    val dbeValue = new DatabaseEntry()
    var txn =
      if (needTXN)
        env.beginTransaction(null,null)
      else
        null
    val cur =
      if (needTXN)
        ns.db.openCursor(txn,null)
      else
        ns.db.openCursor(null, null)

    var status: OperationStatus =
      if(!range.backwards && range.minKey == null) {
        //Starting from neg inf and working our way forward
        cur.getFirst(dbeKey, dbeValue, null)
      }
      else if(!range.backwards) {
        //Starting d minKey and working our way forward
        val minKeyDbe = new DatabaseEntry(range.minKey.get)
        cur.getSearchKeyRange(minKeyDbe, dbeValue, null)
      }
      else if(range.maxKey == null) {
        //Starting from inf and working our way backwards
        cur.getLast(dbeKey, dbeValue, null)
      }
      else { //Starting from maxKey and working our way back
        // Check if maxKey is past the last key in the database, if so start from the end
        val maxKeyDbe = new DatabaseEntry(range.maxKey.get)
        if(cur.getSearchKeyRange(maxKeyDbe, dbeValue, null) == OperationStatus.NOTFOUND)
          cur.getLast(dbeKey, dbeValue, null)
        else
          OperationStatus.SUCCESS
      }
    logger.debug("status: " + status)

    //var toSkip: Int = if(range.offset == null) -1 else range.offset.intValue()
    //var remaining: Int = if(range.limit == null) -1 else range.limit.intValue()
    var toSkip = range.offset match {
        case None       => -1
        case Some(i) => i
    }
    var remaining = range.limit match {
        case None       => -1
        case Some(i) => i
    }

    if (status == OperationStatus.SUCCESS) {
      if(!range.backwards) {
        logger.debug("skipping: " + toSkip)
        while(toSkip > 0 && status == OperationStatus.SUCCESS) {
          status = cur.getNext(dbeKey, dbeValue, null)
          toSkip -= 1
        }

        status = cur.getCurrent(dbeKey, dbeValue, null)
        while(status == OperationStatus.SUCCESS &&
              remaining != 0 &&
                (range.maxKey == null || ns.comp.compare(range.maxKey.get, dbeKey.getData) > 0)) {
                  func(dbeKey, dbeValue,cur)
                  status = cur.getNext(dbeKey, dbeValue, null)
                  remaining -= 1
                }
      }
      else {
        while(toSkip > 0 && status == OperationStatus.SUCCESS) {
          status = cur.getPrev(dbeKey, dbeValue, null)
          toSkip -= 1
        }

        status = cur.getCurrent(dbeKey, dbeValue, null)
        while(status == OperationStatus.SUCCESS &&
              remaining != 0 &&
              (range.minKey == null || ns.comp.compare(range.minKey.get, dbeKey.getData) < 0)) {
                func(dbeKey, dbeValue,cur)
                status = cur.getPrev(dbeKey, dbeValue, null)
                remaining -= 1
              }
      }
    }
    if (finalFunc != null)
      finalFunc(cur)
    cur.close
    if (txn != null) txn.commit
  }

  def openNamespace(ns: String, partition: String): Unit = {
    namespaces.synchronized {
      if (!namespaces.contains(ns)) {
        val nsRoot = root("namespaces").get(ns) match {
          case Some(nsr) => nsr
          case None => throw new RuntimeException("Attempted to open namespace that doesn't exist in zookeeper: " + ns)
        }
        val keySchema = new String(nsRoot("keySchema").data)

        val comp = new AvroComparator(keySchema)
        val dbConfig = new DatabaseConfig
        dbConfig.setAllowCreate(true)
        dbConfig.setBtreeComparator(comp)
        dbConfig.setTransactional(true)

        namespaces += ((ns, Namespace(env.openDatabase(null, ns, dbConfig), Schema.parse(keySchema), comp)))
        logger.info("namespace " + ns + " opened")

        nsRoot("partitions").apply(partition).getOrCreate("servers").createChild(remoteHandle.toString, remoteHandle.toBytes, CreateMode.EPHEMERAL)
      }
    }
  }

  def shutdown(): Unit = {
    var ok = true
    namespaces.synchronized {
      namespaces.foreach { tuple:(String,Namespace) => {
        val (nsStr, ns) = tuple
        logger.info("Closing: "+nsStr)
        try {
          ns.db.close()
        } catch {
          case dbe:DatabaseException => {
            ok = false
            logger.error("Could not close namespace "+nsStr+": "+dbe)
          }
          case excp => {
            ok = false
            throw excp
          }
        }
        logger.info(if(ok) "[OK]" else "[FAIL]")
      } }
    }
    logger.info("Closing environment")
    try {
      env.close()
    } catch {
      case dbe:DatabaseException => {
        ok = false
        logger.error("Could not close environment: "+dbe)
      }
      case excp => {
        ok = false
        throw excp
      }
    }
    logger.info(if(ok) "[OK]" else "[FAIL]")
  }

  def receiveMessage(src: RemoteNode, msg:Message): Unit = {
    try {
      executor.execute(new Request(src, msg))
    } catch {
      case ree: java.util.concurrent.RejectedExecutionException => {
        val resp = new ProcessingException
        resp.cause = "Thread pool exhausted"
        resp.stacktrace = ree.toString
        replyWithError(src,msg,resp)
      }
      case e: Throwable => {
        logger.error("ProcessingException", e)
        e.printStackTrace
        var cause = e.getCause
        while (cause != null) {
          e.printStackTrace
          cause = e.getCause
        }
        val resp = new ProcessingException
        resp.cause = e.toString()
        resp.stacktrace = e.getStackTrace().mkString("\n")
        replyWithError(src,msg,resp)
      }
    }
  }

  def replyWithError(src:RemoteNode, req:Message, except:ProcessingException):Unit = {
    val resp = new Message
    resp.body = except
    if (req.src == null) {
        // TODO: do something here, since resp.dest cannot be null
    }
    resp.dest = req.src
    resp.id = req.id
    MessageHandler.sendMessage(src, resp)
  }
}
