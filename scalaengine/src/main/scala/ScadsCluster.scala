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
import java.nio.ByteBuffer
import scala.collection.jcl.ArrayList
import java.util.Arrays

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

  def createNamespace[KeyType <: SpecificRecordBase, ValueType <: SpecificRecordBase](ns: String, keySchema: Schema, valueSchema: Schema)(implicit keyType: scala.reflect.Manifest[KeyType], valueType: scala.reflect.Manifest[ValueType]): Namespace[KeyType, ValueType] = {
    createNamespace[KeyType, ValueType](ns, keySchema, valueSchema, List[RemoteNode]())
  }

	def createNamespace[KeyType <: SpecificRecordBase, ValueType <: SpecificRecordBase](ns: String, keySchema: Schema, valueSchema: Schema, servers: List[RemoteNode])(implicit keyType: scala.reflect.Manifest[KeyType], valueType: scala.reflect.Manifest[ValueType]): Namespace[KeyType, ValueType] = {
		val nsRoot = namespaces.createChild(ns, "", CreateMode.PERSISTENT)
		nsRoot.createChild("keySchema", keySchema.toString(), CreateMode.PERSISTENT)
		nsRoot.createChild("valueSchema", valueSchema.toString(), CreateMode.PERSISTENT)

		val partition = nsRoot.getOrCreate("partitions/1")
		val policy = new PartitionedPolicy
		policy.partitions = List(new KeyPartition)

		partition.createChild("policy", policy.toBytes, CreateMode.PERSISTENT)
		partition.createChild("servers", "", CreateMode.PERSISTENT)

    servers.foreach(s => {
      val cr = new ConfigureRequest
      cr.namespace = ns
      cr.partition = "1"
      Sync.makeRequest(s, new Utf8("Storage"), cr)
    })

    new Namespace[KeyType, ValueType](ns, 5000, root)
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

  def getNamespace[KeyType <: SpecificRecordBase, ValueType <: SpecificRecordBase](ns: String)(implicit keyType: scala.reflect.Manifest[KeyType], valueType: scala.reflect.Manifest[ValueType]): Namespace[KeyType, ValueType] = {
    new Namespace[KeyType, ValueType](ns, 5000, root)
  }
}

class Namespace[KeyType <: SpecificRecordBase, ValueType <: SpecificRecordBase](namespace:String, timeout:Int, root: ZooKeeperProxy#ZooKeeperNode)(implicit keyType: scala.reflect.Manifest[KeyType], valueType: scala.reflect.Manifest[ValueType]) {
  /* TODOS:
   * - create the namespace if it doesn't exist
   * - add a check that schema of record matches namespace schema or is at least resolvable to the local schema */
  private val dest = new Utf8("Storage")
  private val logger = Logger.getLogger("Namespace")
  protected val keyClass = keyType.erasure
  protected val valueClass = valueType.erasure
  private val nsNode = root.get("namespaces/"+namespace)
  private val schema = Schema.parse(new String(nsNode.get("keySchema").data))

  private var nodeCache:Array[polServer] = null

  private case class polServer(min:ByteBuffer,max:ByteBuffer,nodes:List[RemoteNode]) extends Comparable[polServer] {
    def compareTo(p:polServer):Int = {
      if (max == null && p.max == null) return 0;
      if (max == null) return 1;
      if (p.max == null) return -1;
      if (!max.hasArray)
        throw new Exception("Can't compare without backing array")
      if (!p.max.hasArray)
        throw new Exception("Can't compare without backing array")
      org.apache.avro.io.BinaryData.compare(max.array, max.position, p.max.array, p.max.position,
                                            schema)
    }
  }

  private class RangeIterator(start:Int, end:Int) extends Iterator[polServer] {
    private var cur = start
    def hasNext():Boolean = {
      cur <= end
    }

    def next():polServer = {
      if (cur > end)
        throw new NoSuchElementException()
      cur += 1
      nodeCache((cur-1))
    }
  }

  private def keyComp(part: ByteBuffer, key: Array[Byte]): Int = {
      if (part == null && key == null) return 0;
      if (part == null) return -1;
      if (key == null) return 1;
    if (!part.hasArray)
      throw new Exception("Can't compare without backing array")
    org.apache.avro.io.BinaryData.compare(part.array(), part.position, key, 0, schema)
  }

  private def updateNodeCache():Unit = {
    val partitions = nsNode.get("partitions").updateChildren(false)
    var ranges:Int = 0
    partitions.map(part=>{
      val policyData = 	nsNode.get("partitions/"+part._1+"/policy").updateData(false)
      val policy = new PartitionedPolicy
      policy.parse(policyData)
      ranges += policy.partitions.size.toInt
    })
    nodeCache = new Array[polServer](ranges)
    var idx = 0
    partitions.map(part=>{
      val policyData = 	nsNode.get("partitions/"+part._1+"/policy").updateData(false)
      val policy = new PartitionedPolicy
      policy.parse(policyData)
		  val iter = policy.partitions.iterator
      val nodes = nsNode.get("partitions/"+part._1+"/servers").updateChildren(false).toList.map(ent=>{
        new RemoteNode(ent._1,Integer.parseInt(new String(ent._2.data)))
      }) 
		  while (iter.hasNext) {
			  val part = iter.next
        nodeCache(idx) = new polServer(part.minKey,part.maxKey,nodes)
        idx += 1
      }
		})
    Arrays.sort(nodeCache,null)
  }

  private def idxForKey(key:SpecificRecordBase):Int = {
    if (nodeCache == null)
      updateNodeCache
    val polKey = new polServer(null,key.toBytes,Nil)
    val bpos = Arrays.binarySearch(nodeCache,polKey,null)
    if (bpos < 0) 
      ((bpos+1) * -1)
    else if (bpos == nodeCache.length)
      bpos
    else
      bpos + 1
  }

  def serversForKey(key:SpecificRecordBase):List[RemoteNode] = {
    val idx = idxForKey(key)
    // validate that we don't have a gap
    if (keyComp(nodeCache(idx).min,key.toBytes) > 0) {
      logger.warn("Gap in partitions, returning empty server list")
      Nil
    } else
      nodeCache(idx).nodes
  }

  private def splitRange(startKey:SpecificRecordBase,endKey:SpecificRecordBase):RangeIterator = {
    val sidx = 
      if (startKey == null)
        0
      else
        idxForKey(startKey)
    val eidx = 
      if (endKey == null)
        nodeCache.length - 1
      else
        idxForKey(endKey) 
    // Check if this just makes the range and -1 if so
    new RangeIterator(sidx,eidx)
  }

	private def keyInPolicy(policy:Array[Byte], key: SpecificRecordBase):Boolean = {
		val pdata = new PartitionedPolicy
		pdata.parse(policy)
		val iter = pdata.partitions.iterator
    val kdata = key.toBytes
		while (iter.hasNext) {
			val part = iter.next
      if ( (part.minKey == null ||
            keyComp(part.minKey,kdata) >= 0) &&
           (part.maxKey == null ||
            keyComp(part.maxKey,kdata) < 0) )
        return true
		}
    false
	}

  def serversForKeySlow(key:SpecificRecordBase):List[RemoteNode] = {
    val partitions = nsNode.get("partitions").updateChildren(false)
    partitions.map(part=>{
      val policyData = 	nsNode.get("partitions/"+part._1+"/policy").updateData(false)
      if (keyInPolicy(policyData,key)) {
        nsNode.get("partitions/"+part._1+"/servers").updateChildren(false).toList.map(ent=>{
          new RemoteNode(ent._1,Integer.parseInt(new String(ent._2.data)))
        }) 
      } else
        Nil
		}).toList.flatten(n=>n).removeDuplicates
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

  private def applyToSet(nodes:List[RemoteNode], func:(RemoteNode) => AnyRef, repliesRequired:Int):AnyRef = {
    if (repliesRequired > nodes.size) 
      logger.warn("Asked for " + repliesRequired + " but only passed a list of "+nodes.size+ " this will timeout")
    var c = repliesRequired
    var ret:AnyRef = null
    nodes.foreach(node => {
      val a = new Actor {
        self.trapExit = true
        def act() {
          exit(func(node))
        }
      }
      link(a)
      a.start
    })
    while(c > 0) {
      receiveWithin(timeout) {
        case Exit(act,reason) => {
          c -= 1
          ret = reason
        }
        case TIMEOUT => {
          logger.warn("Timeout waiting for actors to exit")
          // force exit from here, TODO: shoot actors somehow?
          c = 0
        }
        case msg =>
          logger.warn("Unexpected message waiting for actor exits: "+msg)
      }
    }
    ret
  }

  def put[K <: KeyType, V <: ValueType](key: K, value: V): Unit = {
    val nodes = serversForKey(key)
    val pr = new PutRequest
    pr.namespace = namespace
    pr.key = key.toBytes
    pr.value = value.toBytes
    applyToSet(nodes,(rn)=>{doReq(rn,pr)},nodes.size)
  }

  def getBytes[K <: KeyType](key: K): java.nio.ByteBuffer = {
    val nodes = serversForKey(key)
    val gr = new GetRequest
    gr.namespace = namespace
    gr.key = key.toBytes
    applyToSet(nodes,
               (rn)=> {
                 doReq(rn,gr) match {
                   case rec:Record =>
                     rec.value
                   case other => {
                     if (other == null)
                       null
                     else
                       throw new Throwable("Invalid return type from get: "+other)
                   }
                 }
               },
               1) match { // we pass one here to return after the first reply
      case bb:java.nio.ByteBuffer => bb
      case other => null
    }
  }

  def get[K <: KeyType, V <: ValueType](key: K, oldValue: V): Option[V] = {
    val bb = getBytes(key)
    if (bb != null) {
      oldValue.parse(bb)
      Some(oldValue)
    }
    else
      None
  }

  def get[K <: KeyType](key: K): Option[ValueType] = {
    val retValue = valueClass.newInstance().asInstanceOf[ValueType]
    get(key, retValue)
  }

  def flatMap[RetType <: SpecificRecordBase](func: (KeyType, ValueType) => List[RetType])(implicit retType: scala.reflect.Manifest[RetType]): Seq[RetType] = {
    class ResultSeq extends Actor with Seq[RetType] {
      val retClass = retType.erasure
      val result = new SyncVar[List[RetType]]

      def apply(ordinal: Int): RetType = result.get.apply(ordinal)
      def length: Int = result.get.length
      def elements: Iterator[RetType] = result.get.elements

      def act(): Unit = {
        val id = MessageHandler.registerActor(self)
        val ranges = splitRange(null, null).counted
        var partialElements = List[RetType]()
        val msg = new Message
        val req = new FlatMapRequest
        msg.src = new java.lang.Long(id)
        msg.dest = dest
        msg.body = req
        req.namespace = namespace
        req.keyType = keyClass.getName
        req.valueType = valueClass.getName
        req.closure = Closure(func)

        ranges.foreach(r => {
          MessageHandler.sendMessage(r.nodes.first, msg)
        })

        var remaining = ranges.count
        loop {
          reactWithin(1000) {
            case (_, msg: Message) => {
              val resp = msg.body.asInstanceOf[FlatMapResponse]

              //TODO: Use scala idioms for iteration
              val records = resp.records.iterator()
              while(records.hasNext) {
                val rec = retClass.newInstance.asInstanceOf[RetType]
                rec.parse(records.next)
                partialElements += rec
              }

              println(partialElements)

              remaining -= 1
              if(remaining <= 0) {
                result.set(partialElements)
                MessageHandler.unregisterActor(id)
              }
            }
            case TIMEOUT => result.set(null)
            case m => logger.fatal("Unexpected message: " + m)
          }
        }
      }
    }

    (new ResultSeq).start.asInstanceOf[Seq[RetType]]
  }

  /* works for one node right now, will need to split the requests across multiple nodes
   * in the future */
  def bulkPut(start:Int, end:Int, valBytes:Int): Long = {
    val srec = new IntRec
    val erec = new IntRec
    srec.f1 = start
    erec.f1 = end
    val rangeIt = splitRange(srec,erec)
    var c = 0
    val startTime = System.currentTimeMillis()
    while(rangeIt.hasNext()) {
      val polServ = rangeIt.next
      c+=1
      val a = new Actor {
        self.trapExit = true
        def act() {
          val id = MessageHandler.registerActor(self)
          val myId = new java.lang.Long(id)
          val rng = new KeyRange
          val value = "x"*valBytes
          rng.minKey=polServ.min
          rng.maxKey=polServ.max
          val csr = new CopyStartRequest
          csr.ranges = new AvroArray[KeyRange](1024, Schema.createArray((new KeyRange).getSchema))
          csr.ranges.add(rng)
          csr.namespace = namespace
          applyToSet(polServ.nodes,
                     (rn)=> {
                       doReq(rn,csr) match {
                         case tsr: TransferStartReply => {
                           logger.warn("Got TransferStartReply, sending data")
                           val buffer = new AvroArray[Record](100, Schema.createArray((new Record).getSchema))
                           val sendIt = new SendIter(rn,id,tsr.recvActorId,buffer,100,0,logger)
                           var recsSent:Long = 0
                           val sirec = new IntRec
                           val eirec = new IntRec
                           sirec.parse(polServ.min)
                           eirec.parse(polServ.max)
                           for (i <- (sirec.f1 to eirec.f1)) {
                             val rec = new Record
                             sirec.f1 = i
                             rec.key = sirec.toBytes
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
                           while(true) {
                             receiveWithin(timeout) {
                               case (rn:RemoteNode, msg:Message)  => msg.body match {
                                 case bda:BulkDataAck => {
                                   // don't need to do anything, we're ignoring these
                                 }
                                 case ric:RecvIterClose => { // should probably have a status here at some point
                                   logger.warn("Got close, all done")
                                   MessageHandler.unregisterActor(id)
                                   exit
                                 }
                                 case msgb => {
                                   logger.warn("Copy end loop got unexpected message body: "+msgb)
                                 }
                               }
                               case TIMEOUT => {
                                 logger.warn("Copy end loop timed out waiting for finish")
                                 MessageHandler.unregisterActor(id)
                                 exit("TIMEOUT")
                               }
                               case msg =>
                                 logger.warn("Copy end loop got unexpected message: "+msg)
                             }
                           }
                         }
                         case _ => {
                           logger.warn("Unexpected reply to copy start request")
                           MessageHandler.unregisterActor(id)
                           exit("Unexpected reply to copy start request")
                         }
                       }
                     },
                     polServ.nodes.length)
        }
      }
      link(a)
      a.start
    }
    while(c > 0) {
      receiveWithin(timeout) {
        case Exit(act,reason) => 
          c -= 1
        case TIMEOUT => {
          logger.warn("Timeout waiting for actors to exit from bulkPut")
          // force exit from here, TODO: shoot actors somehow?
          c = 0
        }
        case msg =>
          logger.warn("Unexpected message waiting for actor exits from bulkPut: "+msg)
      }
    }
    val endTime = System.currentTimeMillis
    endTime-startTime
  }

}
