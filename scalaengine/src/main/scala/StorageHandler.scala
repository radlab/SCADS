package edu.berkeley.cs.scads.storage

import java.util.Comparator
import java.util.concurrent.{BlockingQueue, ArrayBlockingQueue, ThreadPoolExecutor, TimeUnit}
import java.nio.ByteBuffer
import java.io.ByteArrayInputStream

import scala.actors._
import scala.actors.Actor._

import org.apache.log4j.Logger
import com.sleepycat.je.{Cursor,Database, DatabaseConfig, DatabaseEntry, Environment, LockMode, OperationStatus, Durability, Transaction}

import edu.berkeley.cs.scads.comm._
import edu.berkeley.cs.scads.comm.Conversions._

import org.apache.avro.generic.GenericData.{Array => AvroArray}
import org.apache.avro.Schema
import org.apache.avro.util.Utf8
import org.apache.avro.generic.{GenericDatumReader,GenericData}
import org.apache.avro.io.BinaryDecoder

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

class RecvIter(id:java.lang.Long, logger:Logger) {
  implicit def mkDbe(buff: ByteBuffer): DatabaseEntry = new DatabaseEntry(buff.array, buff.position, buff.remaining)

  def doRecv(recFunc:(Record) => Unit, finFunc:() => Unit) {
    loop {
      react {
				case (rn:RemoteNode, msg: Message) => msg.body match {
          case bd:BulkData => {
            logger.warn("Got bulk data, inserting")
            val it:java.util.Iterator[Record] = bd.records.records.iterator
            while(it.hasNext) {
              val rec = it.next
              recFunc(rec)
            }
            val bda = new BulkDataAck
            bda.seqNum = bd.seqNum
            bda.sendActorId = id.longValue
            val msg = new Message
            msg.body = bda
            msg.dest = new java.lang.Long(bd.sendActorId)
            msg.src = id
            MessageHandler.sendMessage(rn,msg)
            logger.debug("Done and acked")
          }
          case ric:TransferFinished => {
            logger.warn("Got copy finished")
            finFunc()
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

class RecvPullIter(id:java.lang.Long, logger:Logger) extends Iterator[Record] {
  implicit def mkDbe(buff: ByteBuffer): DatabaseEntry = new DatabaseEntry(buff.array, buff.position, buff.remaining)
  private var curRecs:java.util.Iterator[Record] = null
  private var done = false

  private def nextBuf() = {
    if (!done) {
      receive {
			  case (rn:RemoteNode, msg: Message) => msg.body match {
          case bd:BulkData => {
            curRecs = bd.records.records.iterator
            val bda = new BulkDataAck
            bda.seqNum = bd.seqNum
            bda.sendActorId = id.longValue
            val msg = new Message
            msg.body = bda
            msg.dest = new java.lang.Long(bd.sendActorId)
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

class SendIter(targetNode:RemoteNode, id:java.lang.Long, receiverId:java.lang.Long, buffer:AvroArray[Record], capacity:Int, logger:Logger) {
  private var windowLeft = 50 // we'll allow 50 un-acked bulk messages for now
  private var seqNum = 0

  def flush() {
    val bd = new BulkData
    bd.seqNum = seqNum
    seqNum += 1
    bd.sendActorId = id.longValue
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
  }

  def put(rec:Record): Unit = {
    while (windowLeft <= 0) { // not enough acks processed
      reactWithin(60000) { // we'll allow 1 minute for an ack
        case (rn:RemoteNode, bda:BulkDataAck) => 
          windowLeft += 1
        case TIMEOUT => {
          logger.warn("SendIter timed out waiting for ack")
          exit()
        }
        case _ =>
          logger.warn("SendIter got unexpected message")
      }
    }
    buffer.add(rec)
    if (buffer.size >= capacity)  // time to send
      flush
  }
}

class StorageHandler(env: Environment, root: ZooKeeperProxy#ZooKeeperNode) extends ServiceHandler {
  case class Namespace(db: Database, keySchema: Schema, comp: AvroComparator)
  var namespaces: Map[String, Namespace] = new scala.collection.immutable.HashMap[String, Namespace]

  val outstandingRequests = new ArrayBlockingQueue[Runnable](1024)
  val executor = new ThreadPoolExecutor(5, 20, 30, TimeUnit.SECONDS, outstandingRequests)

  implicit def mkDbe(buff: ByteBuffer): DatabaseEntry = new DatabaseEntry(buff.array, buff.position, buff.remaining)
  implicit def mkByteBuffer(dbe: DatabaseEntry):ByteBuffer = ByteBuffer.wrap(dbe.getData, dbe.getOffset, dbe.getSize).compact

  private val logger = Logger.getLogger("StorageHandler")

  private def decodeKey(ns:Namespace, dbe:DatabaseEntry): GenericData.Record = {
    val decoder = new BinaryDecoder(new ByteArrayInputStream(dbe.getData))
    val reader = new GenericDatumReader[GenericData.Record](ns.keySchema)
    reader.read(null,decoder)
  }

  class Request(src: RemoteNode, req: Message) extends Runnable {
    def reply(body: AnyRef) = {
      val resp = new Message
      resp.body = body
      resp.dest = req.src
	    resp.id = req.id
      MessageHandler.sendMessage(src, resp)
    }

    val process: PartialFunction[Object, Unit] = {

      case cr: ConfigureRequest => {
        openNamespace(cr.namespace, cr.partition)
        reply(null)
      }

      case gr: GetRequest => {
        val ns = namespaces(gr.namespace)
        val dbeKey: DatabaseEntry = gr.key
        val dbeValue = new DatabaseEntry

        //println("getting key: " + gr.key)
        //println("getting key: " + dbeKey)

        ns.db.get(null, dbeKey, dbeValue, LockMode.READ_COMMITTED)
        if (dbeValue.getData != null) {
          val retRec = new Record
          //println("returning Key: " + dbeKey)
          //println("returning Value: " + dbeValue)
          //println("returning Key: " + mkByteBuffer(dbeKey))
          //println("returning Value: " + mkByteBuffer(dbeValue))

          retRec.key = dbeKey.getData
          retRec.value = dbeValue.getData

          reply(retRec)
        } else {
          //logger.debug("returning no value mapping")
          reply(null)
        }
      }

      case pr: PutRequest => {
        val ns = namespaces(pr.namespace)
        val key: DatabaseEntry = pr.key
        val txn = env.beginTransaction(null, null)

        //println("putting key: " + key)
        //if (pr != null && pr.value != null)
        //  println("putting value: " + new String(pr.value.array, pr.value.array.position, pr.value.array.remaining))

        if(pr.value == null)
          ns.db.delete(txn, key)
        else
          ns.db.put(txn, key, pr.value)

        txn.commit(Durability.COMMIT_NO_SYNC)
        reply(null)
      }

      case tsr: TestSetRequest => {
        val ns = namespaces(tsr.namespace)
        val txn = env.beginTransaction(null, null)
        val dbeKey: DatabaseEntry = tsr.key
        val dbeEv = new DatabaseEntry()

        /* Set up partial get if the existing value specifies a prefix length */
        if(tsr.prefixMatch) {
          dbeEv.setPartial(true)
          dbeEv.setPartialLength(tsr.expectedValue.position)
        }

        /* Get the current value */
        ns.db.get(txn, dbeKey, dbeEv, LockMode.READ_COMMITTED)
        val expValue: DatabaseEntry = tsr.expectedValue match {
          case null => null
          case _    => tsr.expectedValue
        }

        if((dbeEv.getData == null && tsr.expectedValue != null) ||
           (dbeEv.getData != null && tsr.expectedValue == null) ||
           (dbeEv.getData != null && tsr.expectedValue != null && !dbeEv.equals(expValue))) {
             /* Throw exception if expected value doesnt match present value */
             txn.abort
             logger.warn("TSET FAILURE")
             val tsf = new TestAndSetFailure
             tsf.key = dbeKey
             tsf.currentValue = dbeEv
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
             reply(null)
           }
      }

      case grr: GetRangeRequest => {
        val ns = namespaces(grr.namespace)
        val recordSet = new RecordSet
        recordSet.records = new AvroArray[Record](1024, Schema.createArray((new Record).getSchema))
        logger.warn("ns: " + ns)
        logger.warn("grr.range: " + grr.range)
        iterateOverRange(ns, grr.range, false, (key, value, cursor) => {
          val rec = new Record
          rec.key = key.getData
          rec.value = value.getData
          recordSet.records.add(rec)
          logger.warn("added rec: " + rec)
          logger.warn("rec.key: " + rec.key)
          logger.warn("rec.value: " + rec.value)
          logger.warn("keyBytes.length: " + key.getData.length)
          logger.warn("valueBytes.length: " + value.getData.length)
        })
        logger.warn("recordSet: " + recordSet) 
        reply(recordSet)
      }

      case rrr: RemoveRangeRequest => {
        val ns = namespaces(rrr.namespace)
        iterateOverRange(ns, rrr.range, true, (key, value, cursor) => {
          cursor.delete()
        })
        reply(null)
      }

      case crr: CountRangeRequest => {
        val ns = namespaces(crr.namespace)
        var c = 0
        iterateOverRange(ns, crr.range, false, (key, value, cursor) => {
          c += 1
        })
        reply(int2Integer(c))
      }

      case crr: CopyRangeRequest => {
        val ns = namespaces(crr.namespace)
        val act = actor {
          val msg = new Message
          val req = new CopyStartRequest
          req.namespace = crr.namespace
          req.range = crr.range
          val scId = MessageHandler.registerActor(self)
          val myId = new java.lang.Long(scId)
			    msg.src = myId
          msg.dest = new Utf8("Storage")
          msg.body = req
          val rn = new RemoteNode(crr.destinationHost, crr.destinationPort)
			    MessageHandler.sendMessage(rn, msg)
			    reactWithin(60000) {
				    case (rn:RemoteNode, msg: Message) => msg.body match {
              case csr: TransferStartReply => {
                logger.debug("Got TransferStartReply, sending data")
                val buffer = new AvroArray[Record](100, Schema.createArray((new Record).getSchema))
                val sendIt = new SendIter(rn,myId,csr.recvActorId,buffer,100,logger)
                iterateOverRange(ns, crr.range, false, (key, value, cursor) => {
                  val rec = new Record
                  rec.key = key.getData
                  rec.value = value.getData
                  sendIt.put(rec)
                })
                sendIt.flush
                val fin = new TransferFinished
                fin.sendActorId = myId.longValue
                msg.src = myId
                msg.dest = new java.lang.Long(csr.recvActorId)
                msg.body = fin
                MessageHandler.sendMessage(rn,msg)
                logger.debug("TransferFinished and sent")
                MessageHandler.unregisterActor(scId)
                reply(null)
              }
              case _ => {
                logger.warn("Unexpected reply to copy start request")
                MessageHandler.unregisterActor(scId)
                exit()
              }
				    }
				    case TIMEOUT => {
              logger.warn("Timed out waiting to start a range copy")
              MessageHandler.unregisterActor(scId)
              exit
            }
				    case msg => { 
              logger.warn("Unexpected message: " + msg)
              MessageHandler.unregisterActor(scId)
              exit
            }
			    }
        }
        null
      }

      case srr: SyncRangeRequest => {
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
              val myId = new java.lang.Long(scId)
              val recvIt = new RecvIter(myId,logger)
              val tsr = new TransferStartReply 
              tsr.recvActorId = myId.longValue
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
          val recvActId = MessageHandler.registerActor(recvAct)
          recvAct.scId = recvActId
          req.recvIterId = recvActId
          val scId = MessageHandler.registerActor(self)
          val myId = new java.lang.Long(scId)
			    msg.src = myId
          msg.dest = new Utf8("Storage")
          msg.body = req
          val rn = new RemoteNode(srr.destinationHost, srr.destinationPort)
			    MessageHandler.sendMessage(rn, msg)
			    reactWithin(60000) {
				    case (rn:RemoteNode, msg: Message) => msg.body match {
              case csr: TransferStartReply => {
                logger.debug("Got TransferStartReply, sending data")
                recvAct.start
                val buffer = new AvroArray[Record](100, Schema.createArray((new Record).getSchema))
                val sendIt = new SendIter(rn,myId,csr.recvActorId,buffer,100,logger)
                iterateOverRange(ns, srr.range, false, (key, value, cursor) => {
                  val rec = new Record
                  rec.key = key.getData
                  rec.value = value.getData
                  sendIt.put(rec)
                })
                sendIt.flush
                val fin = new TransferFinished
                fin.sendActorId = myId.longValue
                msg.src = myId
                msg.dest = new java.lang.Long(csr.recvActorId)
                msg.body = fin
                MessageHandler.sendMessage(rn,msg)
                logger.debug("TransferFinished and sent")
                // now we just wait for the receive iter to close
                loop {
                  react {
                    case Exit(act,reas) => {
                      logger.warn("Got exit from recv, cleaning up") // TODO: Debug
                      MessageHandler.unregisterActor(scId)
                      reply(null)
                      exit()
                    }
                    case (rn:RemoteNode, msg: Message) => msg.body match {
                      case bda:BulkDataAck =>
                        logger.warn("BulkAck. dropped since we're done sending") // TODO: Debug
                      case other => {
                        logger.warn("Unexpected message waiting for exit in sync range: "+other)
                        MessageHandler.unregisterActor(scId)
                        exit()
                      }
                    }
                    case msg => {
                      logger.warn("Got unexpected message waiting for exit in sync range: "+msg)
                      MessageHandler.unregisterActor(scId)
                      reply(null)
                      exit()
                    }
                  }
                }
              }
              case _ => {
                logger.warn("Unexpected reply to sync start request")
                MessageHandler.unregisterActor(scId)
                exit()
              }
				    }
				    case TIMEOUT => {
              logger.warn("Timed out waiting to start a range sync")
              MessageHandler.unregisterActor(scId)
              exit
            }
				    case msg => { 
              logger.warn("Unexpected message: " + msg)
              MessageHandler.unregisterActor(scId)
              exit
            }
			    }
        }
        null
      }

      case csreq:CopyStartRequest => { 
        val ns = namespaces(csreq.namespace)
        actor {
          val scId = MessageHandler.registerActor(self)
          val myId = new java.lang.Long(scId)
          val recvIt = new RecvIter(myId,logger)
          val tsr = new TransferStartReply 
          tsr.recvActorId = myId.longValue
          val msg = new Message
          msg.src = myId
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
          MessageHandler.unregisterActor(scId)
        }
      }

      case ssreq:SyncStartRequest => {
        val ns = namespaces(ssreq.namespace)
        actor {
          val scId = MessageHandler.registerActor(self)
          val myId = new java.lang.Long(scId)
          val recvIt = new RecvPullIter(myId,logger)
          val buffer = new AvroArray[Record](100, Schema.createArray((new Record).getSchema)) 
          val sendIt = new SendIter(src,myId,ssreq.recvIterId,buffer,100,logger)
          val tsr = new TransferStartReply
          tsr.recvActorId = myId.longValue
          val msg = new Message
          msg.src = myId
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
          fin.sendActorId = myId.longValue
          msg.src = myId
          msg.dest = new java.lang.Long(ssreq.recvIterId)
          msg.body = fin
          MessageHandler.sendMessage(src,msg)
          logger.debug("TransferFinished and sent for Sync")
          MessageHandler.unregisterActor(scId)
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
    logger.warn("entering iterateOverRange")
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
        cur.getSearchKeyRange(range.minKey, dbeValue, null)
      }
      else if(range.maxKey == null) {
        //Starting from inf and working our way backwards
        cur.getLast(dbeKey, dbeValue, null)
      }
      else { //Starting from maxKey and working our way back
        // Check if maxKey is past the last key in the database, if so start from the end
        if(cur.getSearchKeyRange(range.maxKey, dbeValue, null) == OperationStatus.NOTFOUND)
          cur.getLast(dbeKey, dbeValue, null)
        else
          OperationStatus.SUCCESS
      }
    logger.warn("status: " + status)

    var toSkip: Int = if(range.offset == null) -1 else range.offset.intValue()
    var remaining: Int = if(range.limit == null) -1 else range.limit.intValue()

    if(!range.backwards) {
      logger.warn("skipping: " + toSkip)
      while(toSkip > 0 && status == OperationStatus.SUCCESS) {
        status = cur.getNext(dbeKey, dbeValue, null)
        toSkip -= 1
      }

      status = cur.getCurrent(dbeKey, dbeValue, null)
      while(status == OperationStatus.SUCCESS &&
            remaining != 0 &&
            (range.maxKey == null || ns.comp.compare(range.maxKey, dbeKey.getData) > 0)) {
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
            (range.minKey == null || ns.comp.compare(range.minKey, dbeKey.getData) < 0)) {
              func(dbeKey, dbeValue,cur)
              status = cur.getPrev(dbeKey, dbeValue, null)
              remaining -= 1
            }
    }
    if (finalFunc != null)
      finalFunc(cur)
    cur.close
    if (txn != null) txn.commit
  }

  def openNamespace(ns: String, partition: String): Unit = {
    namespaces.synchronized {
      val nsRoot = root.get("namespaces").get(ns)
      val keySchema = new String(nsRoot("keySchema").data)
      //val policy = partition("policy").data
      //val partition = nsRoot("partitions").get(partition)

      val comp = new AvroComparator(keySchema)
      val dbConfig = new DatabaseConfig
      dbConfig.setAllowCreate(true)
      dbConfig.setBtreeComparator(comp)
      dbConfig.setTransactional(true)

      namespaces += ((ns, Namespace(env.openDatabase(null, ns, dbConfig), Schema.parse(keySchema), comp)))
      logger.info("namespace " + ns + " created")
    }
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
    resp.dest = req.src
	resp.id = req.id
	MessageHandler.sendMessage(src, resp)
  }
}
