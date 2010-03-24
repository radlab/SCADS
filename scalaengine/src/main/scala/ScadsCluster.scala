package edu.berkeley.cs.scads.storage

import edu.berkeley.cs.scads.comm._
import edu.berkeley.cs.scads.comm.Conversions._

import org.apache.avro.Schema

import org.apache.zookeeper.CreateMode


import org.apache.avro.specific.SpecificRecordBase
import org.apache.avro.util.Utf8
import scala.actors._
import scala.actors.Actor._
import edu.berkeley.cs.scads.test.IntRec
import org.apache.log4j.Logger
import org.apache.avro.generic.GenericData.{Array => AvroArray}

class ScadsCluster(root: ZooKeeperProxy#ZooKeeperNode) {
	val namespaces = root.getOrCreate("namespaces")

  /* If namespace exists, just return false, otherwise, create namespace and return true */
  def createNamespaceIfNotExists(ns: String, keySchema: Schema, valueSchema: Schema): Boolean = {
    val nsRoot = 
      try {
        namespaces.get(ns)
      } catch {
        case nse:java.util.NoSuchElementException =>
          null
        case exp =>
          throw exp
      }
    if (nsRoot == null) {
      createNamespace(ns,keySchema,valueSchema)
      true
    } else
      false
  }

	def createNamespace(ns: String, keySchema: Schema, valueSchema: Schema): Unit = {
		val nsRoot = namespaces.createChild(ns, "", CreateMode.PERSISTENT)
		nsRoot.createChild("keySchema", keySchema.toString(), CreateMode.PERSISTENT)
		nsRoot.createChild("valueSchema", valueSchema.toString(), CreateMode.PERSISTENT)

		val partition = nsRoot.getOrCreate("partitions/1")
		val policy = new PartitionedPolicy
		policy.partitions = List(new KeyPartition)

		partition.createChild("policy", policy.toBytes, CreateMode.PERSISTENT)
		partition.createChild("servers", "", CreateMode.PERSISTENT)
	}
	def addPartition(ns:String, name:String, policy:PartitionedPolicy):Unit = {
		val partition = namespaces.getOrCreate(ns+"/partitions/"+name)

		partition.createChild("policy", policy.toBytes, CreateMode.PERSISTENT)
		partition.createChild("servers", "", CreateMode.PERSISTENT)
	}
	def addPartition(ns:String,name:String):Unit = {
		val policy = new PartitionedPolicy
		policy.partitions = List(new KeyPartition)
		addPartition(ns,name,policy)
	}

  def getNamespace(ns: String): Namespace = {
    new Namespace(ns, 5000, root)
  }
}

class Namespace(namespace:String, timeout:Int, root: ZooKeeperProxy#ZooKeeperNode) {
  /* TODOS:
   * - maybe add a check that schema of record matches namespace schema */
  private val dest = new Utf8("Storage")
  private val logger = Logger.getLogger("Namespace")

  private def serverForKey(key:SpecificRecordBase):RemoteNode = {
    new RemoteNode("localhost",9000)
  }

  private def doReq(rn: RemoteNode, reqBody: Object): Object = {
    var retObj:Object = null
    var retExcp:Throwable = null
		val req = new Message
		req.body = reqBody
    req.dest = dest
    val id = MessageHandler.registerActor(self)
		req.src = new java.lang.Long(id)
		MessageHandler.sendMessage(rn, req)
		receiveWithin(timeout) {
			case (RemoteNode(hostname, port), msg: Message) => msg.body match {
				case exp: ProcessingException => 
          retExcp = new Throwable(exp.cause)
				case obj => 
          retObj = obj
			}
			case TIMEOUT => 
        retExcp = new RuntimeException("Timeout")
			case msg => 
        retExcp = new Throwable("Unexpected message: " + msg)
		}
    MessageHandler.unregisterActor(id)
    if (retExcp != null)
      throw retExcp
    retObj
  }

  def put(key: SpecificRecordBase, value: SpecificRecordBase): Unit = {
    val rn = serverForKey(key)
    val pr = new PutRequest
    pr.namespace = namespace
    pr.key = key.toBytes
    pr.value = value.toBytes
    doReq(rn,pr)
  }

  def get(key: SpecificRecordBase): Record = {
    val rn = serverForKey(key)
    val gr = new GetRequest
    gr.namespace = namespace
    gr.key = key.toBytes
    doReq(rn,gr) match {
      case rec:Record =>
        rec
      case other => {
        if (other == null)
          null
        else
          throw new Throwable("Invalid return type from get: "+other)
      }
    }
  }

  /* works for one node right now, will need to split the requests across multiple nodes
   * in the future */
  def bulkPut(start:Int, end:Int, valBytes:Int): Long = {
    val id = MessageHandler.registerActor(self)
    val myId = new java.lang.Long(id)
    val irec = new IntRec
    val rn = serverForKey(irec)
    val rng = new KeyRange
    val value = "x"*valBytes
    irec.f1 = start
    rng.minKey=irec.toBytes
    irec.f1 = end
    rng.maxKey=irec.toBytes
    val csr = new CopyStartRequest
    csr.ranges = new AvroArray[KeyRange](1024, Schema.createArray((new KeyRange).getSchema))
    csr.ranges.add(rng)
    csr.namespace = namespace
    val startTime = System.currentTimeMillis()
    doReq(rn,csr) match {
      case tsr: TransferStartReply => {
        logger.warn("Got TransferStartReply, sending data")
        val buffer = new AvroArray[Record](100, Schema.createArray((new Record).getSchema))
        val sendIt = new SendIter(rn,id,tsr.recvActorId,buffer,100,0,logger)
        var recsSent:Long = 0
        for (i <- (start to end)) {
          val rec = new Record
          irec.f1 = i
          rec.key = irec.toBytes
          rec.value = value
          sendIt.put(rec)
          recsSent += 1
        }
        sendIt.flush
        val fin = new TransferFinished
        fin.sendActorId = id.longValue
        val msg = new Message
        msg.src = myId
        msg.dest = new java.lang.Long(tsr.recvActorId)
        msg.body = fin
        MessageHandler.sendMessage(rn,msg)
        logger.warn("TransferFinished and sent")
        // now we loop and wait for acks and final cleanup from other side
        var stop = false
        var time:Long = 0
        while(!stop) {
          receiveWithin(timeout) {
            case (rn:RemoteNode, msg:Message)  => msg.body match {
              case bda:BulkDataAck => {
                // don't need to do anything, we're ignoring these
              }
              case ric:RecvIterClose => { // should probably have a status here at some point
                logger.warn("Got close, all done")
                val endTime = System.currentTimeMillis
                MessageHandler.unregisterActor(id)
                time = endTime - startTime
                stop = true
              }
              case msgb =>
                logger.warn("Copy end loop got unexpected message body: "+msgb)
            }
            case TIMEOUT => {
              logger.warn("Copy end loop timed out waiting for finish")
              MessageHandler.unregisterActor(id)
              throw new Throwable("Copy end loop timed out waiting for finish")
            }
            case msg =>
              logger.warn("Copy end loop got unexpected message: "+msg)
          }
        }
        time
      }
      case _ => {
        logger.warn("Unexpected reply to copy start request")
        MessageHandler.unregisterActor(id)
        throw new Throwable("Unexpected reply to copy start request")
      }
    }
  }
   
}
