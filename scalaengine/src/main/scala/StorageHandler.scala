package edu.berkeley.cs.scads.storage

import java.util.Comparator
import java.util.concurrent.{BlockingQueue, ArrayBlockingQueue, ThreadPoolExecutor, TimeUnit}
import java.nio.ByteBuffer
import java.io.ByteArrayInputStream
import java.net.InetAddress

import scala.actors._
import scala.actors.Actor._

import org.apache.log4j.Logger
import com.sleepycat.je.{Cursor,Database, DatabaseConfig, DatabaseException, DatabaseEntry, Environment, LockMode, OperationStatus, Durability, Transaction}

import edu.berkeley.cs.scads.comm._
import edu.berkeley.cs.scads.comm.Conversions._

import org.apache.avro.generic.GenericData.{Array => AvroArray}
import org.apache.avro.Schema
import org.apache.avro.util.Utf8
import org.apache.avro.generic.{GenericDatumReader,GenericData}
import org.apache.avro.io.BinaryDecoder

import org.apache.zookeeper.CreateMode

import com.googlecode.avro.runtime.ScalaSpecificRecord

@serializable
class AvroComparator(val json: String) extends Comparator[Array[Byte]] with java.io.Serializable {
  @transient
  lazy val schema = Schema.parse(json)

  def compare(o1: Array[Byte], o2: Array[Byte]): Int = {
    org.apache.avro.io.BinaryData.compare(o1, 0, o2, 0, schema)
  }

  def compare(o1: ByteBuffer, o2: Array[Byte]): Int = {
    if (!o1.hasArray)
      throw new Exception("Can't compare without backing array")
    org.apache.avro.io.BinaryData.compare(o1.array(), o1.position, o2, 0, schema)
  }

  def compare(o1: Array[Byte], o2: ByteBuffer): Int = {
    if (!o2.hasArray)
      throw new Exception("Can't compare without backing array")
    org.apache.avro.io.BinaryData.compare(o1, 0, o2.array, o2.position, schema)
  }

  override def equals(other: Any): Boolean = other match {
    case ac: AvroComparator => json equals ac.json
    case _ => false
  }
}

class RecvIter(id: ActorId, logger:Logger) {
  implicit def mkDbe(buff: ByteBuffer): DatabaseEntry = new DatabaseEntry(buff.array, buff.position, buff.remaining)

  def doRecv(recFunc:(Record) => Unit, finFunc:() => Unit) {
    loop {
      react {
        case (rn:RemoteNode, msg: Message) => msg.body match {
          case bd:BulkData => {
            logger.debug("Got bulk data, inserting")
            val it = bd.records.records.iterator
            while(it.hasNext) {
              val rec = it.next
              recFunc(rec)
            }
            val bda = new BulkDataAck
            bda.seqNum = bd.seqNum
            bda.sendActorId = id
            val msg = new Message
            msg.body = bda
            //msg.dest = new java.lang.Long(bd.sendActorId)
            msg.dest = bd.sendActorId
            msg.src = id
            MessageHandler.sendMessage(rn,msg)
            logger.debug("Done and acked")
          }
          case tf:TransferFinished => {
            logger.debug("Got copy finished")
            finFunc()
            val ric = new RecvIterClose
            ric.sendActorId = id 
            val msg = new Message
            msg.body = ric
            //msg.dest = new java.lang.Long(tf.sendActorId)
            msg.dest = tf.sendActorId
            msg.src = id
            MessageHandler.sendMessage(rn,msg)
            exit()
          }
          case m => {
            logger.warn("RecvIter got unexpected type of message: "+msg.body)
          }
        }
        case msg => {
          logger.warn("RecvIter got unexpected message: "+msg)
        }
      }
    }
  }
}

class RecvPullIter(id: ActorId, logger:Logger) extends Iterator[Record] {
  implicit def mkDbe(buff: ByteBuffer): DatabaseEntry = new DatabaseEntry(buff.array, buff.position, buff.remaining)
  private var curRecs: Iterator[Record] = null
  private var done = false

  private def nextBuf() = {
    if (!done) {
      receive {
        case (rn:RemoteNode, msg: Message) => msg.body match {
          case bd:BulkData => {
            curRecs = bd.records.records.iterator
            val bda = new BulkDataAck
            bda.seqNum = bd.seqNum
            bda.sendActorId = id
            val msg = new Message
            msg.body = bda
            //msg.dest = new java.lang.Long(bd.sendActorId)
            msg.dest = bd.sendActorId
            msg.src = id
            MessageHandler.sendMessage(rn,msg)
          }
          case ric:TransferFinished => {
            logger.debug("RecvPullIter: Got copy finished")
            done = true
            curRecs = null
          }
          case m => {
            logger.warn("RecvPullIter: got unexpected type of message: "+msg.body)
            done = true
            curRecs = null
          }
        }
        case msg => {
          logger.warn("RecvPullIter: got unexpected message: "+msg)
          done = true
          curRecs = null
        }
      }
    }
  }

  def hasNext(): Boolean = {
    if (done) return false
    if (curRecs == null || !(curRecs.hasNext)) nextBuf
    if (done) return false
    return true
  }

  def next(): Record = {
    if (done) return null
    if (curRecs == null || !(curRecs.hasNext)) nextBuf
    if (done) return null
    curRecs.next
  }
}

class SendIter(targetNode:RemoteNode, id: ActorId, receiverId: ActorId, buffer:AvroArray[Record], capacity:Int, speedLimit:Int, logger:Logger) {
  private var windowLeft = 50 // we'll allow 50 un-acked bulk messages for now
  private var seqNum = 0
  private var bytesQueued:Long = 0
  private var bytesSentLast:Long = 0
  private var lastSentTime:Long = 0

  def flush() {
    if (speedLimit != 0) {
      val secs = (System.currentTimeMillis - lastSentTime)/1000.0
      val target = bytesSentLast / speedLimit.toDouble
      if (target > secs)  // if we wanted to take longer than we actually did
        Thread.sleep(((target-secs)*1000).toLong)
    }
    val bd = new BulkData
    bd.seqNum = seqNum
    seqNum += 1
    bd.sendActorId = id
    val rs = new RecordSet
    rs.records = buffer
    bd.records = rs
    val msg = new Message
    msg.dest = receiverId
    msg.src = id
    msg.body = bd
    MessageHandler.sendMessage(targetNode,msg)
    windowLeft -= 1
    buffer.clear
    if (speedLimit != 0) {
      bytesSentLast = bytesQueued
      bytesQueued = 0
      lastSentTime = System.currentTimeMillis()
    }
  }

  def finish(timeout:Int): Unit = {
    flush
    val fin = new TransferFinished
    fin.sendActorId = id
    val msg = new Message
    msg.src = id
    msg.dest = receiverId
    msg.body = fin
    MessageHandler.sendMessage(targetNode,msg)
    // now we loop and wait for acks and final cleanup from other side
    var done = false
    while(!done) {
      receiveWithin(timeout) {
        case (rn:RemoteNode, msg:Message)  => msg.body match {
          case bda:BulkDataAck => {
            // don't need to do anything, we're ignoring these
          }
          case ric:RecvIterClose => { // should probably have a status here at some point
            done = true
          }
          case msgb => {
            logger.warn("Copy end loop got unexpected message body: "+msgb)
          }
        }
          case TIMEOUT => {
            logger.warn("Copy end loop timed out waiting for finish")
            done = true
          }
        case msg =>
          logger.warn("Copy end loop got unexpected message: "+msg)
      }
    }
  }

  def put(rec:Record): Unit = {
    while (windowLeft <= 0) { // not enough acks processed
      receiveWithin(60000) { // we'll allow 1 minute for an ack
        case (rn:RemoteNode, msg:Message)  => msg.body match {
          case bda:BulkDataAck => {
            logger.debug("Got ack, increasing window")
            windowLeft += 1
          }
          case msgb =>
            logger.warn("SendIter got unexpected message body: "+msgb)
        }
        case TIMEOUT => {
          logger.warn("SendIter timed out waiting for ack")
          exit()
        }
        case msg =>
          logger.warn("SendIter got unexpected message: "+msg)
      }
    }
    buffer.add(rec)
    bytesQueued += rec.value.remaining
    if (buffer.size >= capacity)  // time to send
      flush
  }
}

class StorageHandler(env: Environment, root: ZooKeeperProxy#ZooKeeperNode, localAddr:String, port:Int) extends ServiceHandler {
  def this(env: Environment, root: ZooKeeperProxy#ZooKeeperNode) =
    this(env,root,InetAddress.getLocalHost.getHostName,MessageHandler.getLocalPort)

  case class Namespace(db: Database, keySchema: Schema, comp: AvroComparator)
  var namespaces: Map[String, Namespace] = new scala.collection.immutable.HashMap[String, Namespace]

  val outstandingRequests = new ArrayBlockingQueue[Runnable](1024)
  val executor = new ThreadPoolExecutor(5, 20, 30, TimeUnit.SECONDS, outstandingRequests)

  implicit def mkDbe(buff: Array[Byte]): DatabaseEntry = new DatabaseEntry(buff)
  implicit def mkByteBuffer(dbe: DatabaseEntry): Array[Byte] = {
    if(dbe.getOffset == 0)
      return dbe.getData
    else
      throw new RuntimeException("Unimplemented")
  }

  private val logger = Logger.getLogger("StorageHandler")

  class SDRunner(sh: StorageHandler) extends Thread {
    override def run(): Unit = {
      sh.shutdown()
    }
  }
  java.lang.Runtime.getRuntime().addShutdownHook(new SDRunner(this))

  val availServs = root.getOrCreate("availableServers")
  val lport =
    if (port == 0) 
      MessageHandler.getLocalPort
    else
      port
  availServs.createChild(localAddr, (lport+"").getBytes, CreateMode.EPHEMERAL)

  private def decodeKey(ns:Namespace, dbe:DatabaseEntry): GenericData.Record = {
    val decoder = new BinaryDecoder(new ByteArrayInputStream(dbe.getData(),dbe.getOffset(),dbe.getSize()))
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

  private class ShippedClassLoader(ba:ByteBuffer,targetClass:String) extends ClassLoader {
    override def findClass(name:String):Class[_] = {
      if (name.equals(targetClass))
        defineClass(name, ba.array, ba.position, ba.remaining)
      else 
        Class.forName(name)
    }
  }

  private def deserialize(name:String, ba:ByteBuffer):Any = {
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
      resp.body = body
      if (req.src == null) {
        // TODO: do something here, since resp.dest cannot be null
      }
      resp.dest = req.src
      resp.id = req.id
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
          retRec.value = dbeValue

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

        if((dbeEv.getData == null && tsr.expectedValue.isDefined != null) ||
           (dbeEv.getData != null && tsr.expectedValue.isEmpty == null) ||
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
        recordSet.records = new AvroArray[Record](1024, Schema.createArray((new Record).getSchema))
        iterateOverRange(ns, grr.range, false, (key, value, cursor) => {
          val rec = new Record
          rec.key = key.getData
          rec.value = value.getData
          recordSet.records = rec :: recordSet.records
        })
        recordSet.records = recordSet.records.reverse
        reply(recordSet)
      }

      case gpr: GetPrefixRequest => {
        val ns = namespaces(gpr.namespace)

        val tschema = mkSchema(ns.keySchema,gpr.fields)
        val recordSet = new RecordSet
        recordSet.records = new AvroArray[Record](1024, Schema.createArray((new Record).getSchema))
        
        var remaining = gpr.limit match {
          case None       => -1
          case Some(i) => i
        }
        var ascending = gpr.ascending

        val dbeKey:DatabaseEntry = mkDbe(gpr.start)
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
            recordSet.records = rec :: recordSet.records
            stat = 
              if (gpr.ascending)
                cur.getNext(dbeKey, dbeValue, null)
              else
                cur.getPrev(dbeKey, dbeValue, null)
            remaining -= 1
          }
          recordSet.records = recordSet.records.reverse
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

      case crr: CopyRangesRequest => {
        val ns = namespaces(crr.namespace)
        val startTime = System.currentTimeMillis()
        val act = actor {
          val msg = new Message
          val req = new CopyStartRequest
          req.namespace = crr.namespace
          req.ranges = crr.ranges
          val scId = new ActorNumber(MessageHandler.registerActor(self))
          msg.src = scId
          msg.dest = ActorName("Storage")
          msg.body = req
          val rn = new RemoteNode(crr.destinationHost, crr.destinationPort)
          MessageHandler.sendMessage(rn, msg)
          
          // Now reply to requestor with a started message
          val ts = new TransferStarted
          ts.sendActorId = scId
          reply(ts)

          reactWithin(60000) {
            case (rn:RemoteNode, msg: Message) => msg.body match {
              case csr: TransferStartReply => {
                logger.debug("Got TransferStartReply, sending data")
                val buffer = new AvroArray[Record](100, Schema.createArray((new Record).getSchema))
                val sendIt = new SendIter(rn,scId,csr.recvActorId,buffer,100,crr.rateLimit,logger)
                var recsSent:Long = 0
                val rangeIt = crr.ranges.iterator
                while(rangeIt.hasNext) {
                  iterateOverRange(ns, rangeIt.next(), false, (key, value, cursor) => {
                    val rec = new Record
                    rec.key = key.getData
                    rec.value = value.getData
                    sendIt.put(rec)
                    recsSent += 1
                  })
                }
                sendIt.flush
                val fin = new TransferFinished
                fin.sendActorId = scId 
                msg.src = scId
                //msg.dest = new java.lang.Long(csr.recvActorId)
                msg.dest = csr.recvActorId
                msg.body = fin
                MessageHandler.sendMessage(rn,msg)
                logger.debug("TransferFinished and sent")
                // now we loop and wait for acks and final cleanup from other side
                loop {
                  reactWithin(60000) { // we'll allow 1 minute for an ack
                    case (rn:RemoteNode, msg:Message)  => msg.body match {
                      case bda:BulkDataAck => {
                        // don't need to do anything, we're ignoring these
                      }
                      case ric:RecvIterClose => { // should probably have a status here at some point
                        logger.debug("Got close, all done")
                        val endTime = System.currentTimeMillis
                        MessageHandler.unregisterActor(scId.num)
                        val tsm = new TransferSucceeded
                        tsm.sendActorId = scId
                        tsm.recordsSent = recsSent
                        tsm.milliseconds = (endTime - startTime)
                        reply(tsm)
                        exit()
                      }
                      case msgb =>
                        logger.warn("Copy end loop got unexpected message body: "+msgb)
                    }
                    case TIMEOUT => {
                      logger.warn("Copy end loop timed out waiting for finish")
                      val tf = new TransferFailed
                      tf.sendActorId = scId
                      tf.reason = "Copy end loop timed out waiting for finish"
                      reply(tf)
                      exit()
                    }
                    case msg =>
                      logger.warn("Copy end loop got unexpected message: "+msg)
                  }
                }
              }
              case _ => {
                logger.warn("Unexpected reply to copy start request")
                MessageHandler.unregisterActor(scId.num)
                val tf = new TransferFailed
                tf.sendActorId = scId
                tf.reason = "Unexpected reply to copy start request"
                reply(tf)
                exit()
              }
            }
            case TIMEOUT => {
              logger.warn("Timed out waiting to start a range copy")
              MessageHandler.unregisterActor(scId.num)
              val tf = new TransferFailed
              tf.sendActorId = scId
              tf.reason = "Timed out waiting to start a range copy"
              reply(tf)
              exit()
            }
            case msg => { 
              logger.warn("Unexpected message waiting to start range copy: " + msg)
              MessageHandler.unregisterActor(scId.num)
              val tf = new TransferFailed
              tf.sendActorId = scId
              tf.reason = "Unexpected message waiting to start range copy: "+msg
              reply(tf)
              exit()
            }
          }
        }
        null
      }

      case csreq:CopyStartRequest => { 
        val ns = namespaces(csreq.namespace)
        actor {
          val scId = new ActorNumber(MessageHandler.registerActor(self))
          val recvIt = new RecvIter(scId,logger)
          val tsr = new TransferStartReply 
          tsr.recvActorId = scId
          val msg = new Message
          msg.src = scId
          if (req.src == null) {
               // TODO: do something b/c msg.dest cannot be null 
          }
          msg.dest = req.src
          msg.body=tsr
          MessageHandler.sendMessage(src,msg)
          var txn = env.beginTransaction(null,null)
          recvIt.doRecv(
            rec => {
              val key:DatabaseEntry = rec.key
              ns.db.put(txn,key,rec.value)
            },
            () => {
              txn.commit(Durability.COMMIT_NO_SYNC)
            })
          MessageHandler.unregisterActor(scId.num)
        }
      }

      case srr: SyncRangeRequest => {
        srr.method match {
          //case synctype.SIMPLE =>
          //  doSimpleSync(srr)
          //case synctype.MERKLE =>
          //  merkleSyncSink(srr)

          // TODO: no enums for now, will need to fix in future
          case 0 => 
              doSimpleSync(srr)
          case 1 => 
              merkleSyncSink(srr)
        }
      }

      case ssreq:SyncStartRequest => {
        simpleSyncSink(ssreq,src,req)
      }

                 /* 
                    // Strange bug happens when you uncomment this code.
                    // scalac will crash.
      case fmreq: FlatMapRequest => {
        val ns = namespaces(fmreq.namespace)
        var result = new GenericData.Array[ByteBuffer](10, Schema.createArray(Schema.create(Schema.Type.BYTES)))

        val key = Class.forName(fmreq.keyType).asInstanceOf[Class[ScalaSpecificRecord]].newInstance()
        val value = Class.forName(fmreq.valueType).asInstanceOf[Class[ScalaSpecificRecord]].newInstance()
        try {
          val fclass = deserialize(fmreq.codename,fmreq.closure)
          fclass match {
            case cl:Class[_] => {
              val o = cl.newInstance
              val methods = cl.getMethods()
              var midx = 0
              for (i <- (1 to (methods.length-1))) {
                val method = methods(i)
                if (method.getName.indexOf("apply") >= 0) {
                  midx = i
                }
              }
              val method = methods(midx)
              iterateOverRange(ns, new KeyRange, false, (keyBytes, valueBytes, _) => {
                key.parse(keyBytes.getData)
                value.parse(valueBytes.getData)
                method.invoke(o,key,value).asInstanceOf[List[ScalaSpecificRecord]].
                  map(v => ByteBuffer.wrap(v.toBytes)).foreach(result.add)
              })
              
              val resp = new FlatMapResponse
              resp.records = result
              reply(resp)
            }
          }
        } catch {
          case e: Throwable => {
            println("Filter Fail")
            e.printStackTrace()
            val resp = new FlatMapResponse
            reply(resp)
          }
        }
      }
      */

      case filtreq:FilterRequest  => {
        val ns = namespaces(filtreq.namespace)
        val recordSet = new RecordSet
        recordSet.records = new AvroArray[Record](1024, Schema.createArray((new Record).getSchema))
        val key = Class.forName(filtreq.keyType).asInstanceOf[Class[ScalaSpecificRecord]].newInstance()
        val value = Class.forName(filtreq.valueType).asInstanceOf[Class[ScalaSpecificRecord]].newInstance()
        try {
          val fclass = deserialize(filtreq.codename,filtreq.code)
          fclass match {
            case cl:Class[_] => {
              val o = cl.newInstance
              val methods = cl.getMethods()
              var midx = 0
              for (i <- (1 to (methods.length-1))) {
                val method = methods(i)
                if (method.getName.indexOf("apply") >= 0) {
                  midx = i
                }
              }
              val method = methods(midx)
              iterateOverRange(ns, new KeyRange, false, (keyBytes, valueBytes, _) => {
                key.parse(keyBytes.getData)
                value.parse(valueBytes.getData)
                val b = method.invoke(o,key,value).asInstanceOf[Boolean]
                if (b) {
                  val rec = new Record
                  rec.key = keyBytes.getData
                  rec.value = valueBytes.getData
                  recordSet.records = rec :: recordSet.records
                }
              })
              reply(recordSet)
            }
          }
        } catch {
          case e: Throwable => {
            println("Filter Fail")
            e.printStackTrace()
            reply(recordSet)
          }
        }
      }

      case foldreq:FoldRequest  => {
        val ns = namespaces(foldreq.namespace)
        val key = Class.forName(foldreq.keyType).asInstanceOf[Class[ScalaSpecificRecord]].newInstance()
        val value = Class.forName(foldreq.valueType).asInstanceOf[Class[ScalaSpecificRecord]].newInstance()
        var initVals = (
          Class.forName(foldreq.keyType).asInstanceOf[Class[ScalaSpecificRecord]].newInstance(),
          Class.forName(foldreq.valueType).asInstanceOf[Class[ScalaSpecificRecord]].newInstance() 
        )
        initVals._1.parse(foldreq.initValueOne)
        initVals._2.parse(foldreq.initValueTwo)
        try {
          val fclass = deserialize(foldreq.codename,foldreq.code)
          fclass match {
            case cl:Class[_] => {
              val o = cl.newInstance
              val methods = cl.getMethods()
              var midx = 0
              for (i <- (1 to (methods.length-1))) {
                val method = methods(i)
                if (method.getName.indexOf("apply") >= 0) {
                  midx = i
                }
              }
              val method = methods(midx)
              val range = new KeyRange
              if (foldreq.direction == 1)
                range.backwards = true
              iterateOverRange(ns, range, false, (keyBytes, valueBytes, _) => {
                key.parse(keyBytes.getData)
                value.parse(valueBytes.getData)
                initVals = method.invoke(o,initVals,(key,value)).asInstanceOf[(ScalaSpecificRecord,ScalaSpecificRecord)]
              })
              val retRec = new Record
              retRec.key = initVals._1.toBytes
              retRec.value = initVals._2.toBytes
              reply(retRec)
            }
          }
        } catch {
          case e: Throwable => {
            println("Fold Fail")
            e.printStackTrace()
            reply(new Record)
          }
        }
      }

      case foldreq:FoldRequest2L  => {
        val ns = namespaces(foldreq.namespace)
        val key = Class.forName(foldreq.keyType).asInstanceOf[Class[ScalaSpecificRecord]].newInstance()
        val value = Class.forName(foldreq.valueType).asInstanceOf[Class[ScalaSpecificRecord]].newInstance()
        var initVal =  Class.forName(foldreq.initType).asInstanceOf[Class[ScalaSpecificRecord]].newInstance()
        initVal.parse(foldreq.initValue)
        try {
          val fclass = deserialize(foldreq.codename,foldreq.code)
          fclass match {
            case cl:Class[_] => {
              val o = cl.newInstance
              val methods = cl.getMethods()
              var midx = 0
              for (i <- (1 to (methods.length-1))) {
                val method = methods(i)
                if (method.getName.indexOf("apply") >= 0) {
                  midx = i
                }
              }
              val method = methods(midx)
              val range = new KeyRange
              if (foldreq.direction == 1)
                range.backwards = true
              iterateOverRange(ns, range, false, (keyBytes, valueBytes, _) => {
                key.parse(keyBytes.getData)
                value.parse(valueBytes.getData)
                initVal = method.invoke(o,initVal,(key,value)).asInstanceOf[ScalaSpecificRecord]
              })
              val rep = new Fold2Reply
              rep.reply = initVal.toBytes
              reply(rep)
            }
          }
        } catch {
          case e: Throwable => {
            println("Fold2 Fail")
            e.printStackTrace()
            reply(new Record)
          }
        }
      }
      // End of message handling
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

  def doSimpleSync(srr: SyncRangeRequest): Unit = {
    val ns = namespaces(srr.namespace)
    val act = actor {
      self.trapExit = true // need this to catch end of recv actor
      val msg = new Message
      val req = new SyncStartRequest
      req.namespace = srr.namespace
      req.range = srr.range
      // create the recv iter for replies
      val recvAct = new Actor {
        var scId:Long = 0
        def act() {
          val myId = new ActorNumber(scId)
          val recvIt = new RecvIter(myId,logger)
          val tsr = new TransferStartReply 
          tsr.recvActorId = myId
          var txn = env.beginTransaction(null,null)
          recvIt.doRecv(
            rec => {
              val key:DatabaseEntry = rec.key
              ns.db.put(txn,key,rec.value)
            },
            () => {
              txn.commit(Durability.COMMIT_NO_SYNC)
            })
          MessageHandler.unregisterActor(scId)
        }
      }
      link(recvAct)
      val recvActId = new ActorNumber(MessageHandler.registerActor(recvAct))
      recvAct.scId = recvActId.num
      req.recvIterId = recvActId
      val scId = new ActorNumber(MessageHandler.registerActor(self))
      msg.src = scId
      msg.dest = new ActorName("Storage")
      msg.body = req
      val rn = new RemoteNode(srr.destinationHost, srr.destinationPort)
      MessageHandler.sendMessage(rn, msg)
      reactWithin(60000) {
        case (rn:RemoteNode, msg: Message) => msg.body match {
          case csr: TransferStartReply => {
            logger.debug("Got TransferStartReply, sending data")
            recvAct.start
            val buffer = new AvroArray[Record](100, Schema.createArray((new Record).getSchema))
            val sendIt = new SendIter(rn,scId,csr.recvActorId,buffer,100,0,logger)
            iterateOverRange(ns, srr.range, false, (key, value, cursor) => {
              val rec = new Record
              rec.key = key.getData
              rec.value = value.getData
              sendIt.put(rec)
            })
            sendIt.flush
            val fin = new TransferFinished
            fin.sendActorId = scId
            msg.src = scId 
            //msg.dest = new java.lang.Long(csr.recvActorId)
            msg.dest = csr.recvActorId
            msg.body = fin
            MessageHandler.sendMessage(rn,msg)
            logger.debug("TransferFinished and sent")
            // now we just wait for the receive iter to close
            loop {
              react {
                case Exit(act,reas) => {
                  logger.debug("Got exit from recv, cleaning up") 
                  MessageHandler.unregisterActor(scId.num)
                  reply(EmptyResponse())
                  exit()
                }
                case (rn:RemoteNode, msg: Message) => msg.body match {
                  case bda:BulkDataAck =>
                    logger.debug("BulkAck. dropped since we're done sending") // TODO: Debug
                  case other => {
                    logger.warn("Unexpected message waiting for exit in sync range: "+other)
                    MessageHandler.unregisterActor(scId.num)
                    exit()
                  }
                }
                case msg => {
                  logger.warn("Got unexpected message waiting for exit in sync range: "+msg)
                  MessageHandler.unregisterActor(scId.num)
                  reply(EmptyResponse())
                  exit()
                }
              }
            }
          }
          case _ => {
            logger.warn("Unexpected reply to sync start request")
            MessageHandler.unregisterActor(scId.num)
            exit()
          }
        }
        case TIMEOUT => {
          logger.warn("Timed out waiting to start a range sync")
          MessageHandler.unregisterActor(scId.num)
          exit
        }
        case msg => { 
          logger.warn("Unexpected message: " + msg)
          MessageHandler.unregisterActor(scId.num)
          exit
        }
      }
    }
    null
  }

  def simpleSyncSink(ssreq: SyncStartRequest, src: RemoteNode, req: Message): Unit = {
    val ns = namespaces(ssreq.namespace)
    actor {
      val scId = new ActorNumber(MessageHandler.registerActor(self))
      val recvIt = new RecvPullIter(scId,logger)
      val buffer = new AvroArray[Record](100, Schema.createArray((new Record).getSchema)) 
      val sendIt = new SendIter(src,scId,ssreq.recvIterId,buffer,100,0,logger)
      val tsr = new TransferStartReply
      tsr.recvActorId = scId 
      val msg = new Message
      msg.src = scId
      if (req.src == null) {
          // TODO: do something here, since resp.dest cannot be null
      }
      msg.dest = req.src
      msg.body=tsr
      MessageHandler.sendMessage(src,msg)
      val updateIterId = ssreq.recvIterId
      var curRemoteRec:Record = null
      iterateOverRange(ns,ssreq.range, true, (key, value, cursor) => {
        /* Cases are:
         * 1. My key is less = other side is missing my data, send until in another case
         * 2. My key is greater = I'm missing data, loop on recvIt, inserting, until in another case
         * 3. keys are equal = check data, mine greater = send mine, rem greater = insert, equal = do nothing
         * 4. Other side finished, I'm not = other side is missing data, send what i have left
         * 5. I'm finished, other side isn't = (this call will be in finalFunc), just insert the rest
         */
        // case 4
        if (!recvIt.hasNext) {
          val rec = new Record
          rec.key = key.getData
          rec.value = value.getData
          sendIt.put(rec)
        }
        else {
          // so there is at least one more remote record to deal with
          if (curRemoteRec == null)
            curRemoteRec = recvIt.next

          var keyComp = ns.comp.compare(key.getData,curRemoteRec.key)
          if (keyComp < 0) { // case 1
            val rec = new Record
            rec.key = key.getData
            rec.value = value.getData
            sendIt.put(rec)
          }
          else if (keyComp > 0) { // case 
            while(keyComp > 0) {
              val remKey: DatabaseEntry = curRemoteRec.key
              cursor.put(remKey,curRemoteRec.value)
              curRemoteRec = recvIt.next
              if (curRemoteRec != null)
                keyComp = ns.comp.compare(key.getData,curRemoteRec.key)
              else
                keyComp = 0
            }
          }
              else if (keyComp ==  0) { // case 3
                val dataComp = ns.comp.compare(value.getData,curRemoteRec.value)
                if (dataComp < 0) {
                  // remote greater, just insert
                  cursor.put(key,curRemoteRec.value)
                }
                else if (dataComp > 0) {
                  // remote less, send over
                  val rec = new Record
                  rec.key = key.getData
                  rec.value = value.getData
                  sendIt.put(rec)
                }
                curRemoteRec = null // we've handled this one, so we need a new one
              }
        }
      },
      (cursor) => {
        // final func, case 5
        if (curRemoteRec != null) {
          val key: DatabaseEntry = curRemoteRec.key
          cursor.put(key,curRemoteRec.value)
        }
        while (recvIt.hasNext) {
          curRemoteRec = recvIt.next
          val key: DatabaseEntry = curRemoteRec.key
          cursor.put(key,curRemoteRec.value)
        }
      })
      sendIt.flush
      val fin = new TransferFinished
      fin.sendActorId = scId 
      msg.src = scId 
      //msg.dest = new java.lang.Long(ssreq.recvIterId)
      msg.dest = ssreq.recvIterId
      msg.body = fin
      MessageHandler.sendMessage(src,msg)
      logger.debug("TransferFinished and sent for Sync")
      MessageHandler.unregisterActor(scId.num)
    }
  }

  def merkleSyncSink(srr:SyncRangeRequest):Unit = {
    null
  }

  def openNamespace(ns: String, partition: String): Unit = {
    namespaces.synchronized {
      if (!namespaces.contains(ns)) {
        val nsRoot = root.get("namespaces").updateChildren(false).get(ns) match {
          case Some(nsr) => nsr
          case None => throw new RuntimeException("Attempted to open namespace that doesn't exist in zookeeper: " + ns)
        }
        val keySchema = new String(nsRoot("keySchema").data)
        //val policy = new PartitionedPolicy
        //policy.parse((nsRoot("partitions").get(partition))("policy").data)
        val lport =
          if (port == 0) 
            MessageHandler.getLocalPort
          else
            port

        (nsRoot("partitions").get(partition).getOrCreate("servers")).createChild(localAddr, (lport+"").getBytes, CreateMode.EPHEMERAL)
        
        val comp = new AvroComparator(keySchema)
        val dbConfig = new DatabaseConfig
        dbConfig.setAllowCreate(true)
        dbConfig.setBtreeComparator(comp)
        dbConfig.setTransactional(true)
        
        namespaces += ((ns, Namespace(env.openDatabase(null, ns, dbConfig), Schema.parse(keySchema), comp)))
        logger.info("namespace " + ns + " created")
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
