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
import edu.berkeley.cs.scads.test.StringRec
import org.apache.log4j.Logger
import org.apache.avro.generic.GenericData.{Array => AvroArray}
import org.apache.avro.generic.GenericData
import java.nio.ByteBuffer
import scala.collection.jcl.ArrayList
import java.util.Arrays
import scala.concurrent.SyncVar
import scala.tools.nsc.interpreter.AbstractFileClassLoader
import scala.tools.nsc.io.AbstractFile

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

  /* get array of bytes which is the compiled version of cl */
  private def getFunctionCode(cl:AnyRef):java.nio.ByteBuffer = {
    val ldr = cl.getClass.getClassLoader
    ldr match {
      case afcl:AbstractFileClassLoader => {
        // can't use getResourceAsStream here because it always returns null on an AFCL
        val name = cl.getClass.getName
        val rootField = afcl.getClass.getDeclaredField("root")
        rootField.setAccessible(true)
        var file: AbstractFile = rootField.get(afcl).asInstanceOf[AbstractFile]
        val pathParts = name.split("[./]").toList
        for (dirPart <- pathParts.init) {
          file = file.lookupName(dirPart, true)
            if (file == null) {
              throw new ClassNotFoundException(name)
            }
        }
        file = file.lookupName(pathParts.last+".class", false)
        if (file == null) {
          throw new ClassNotFoundException(name)
        }
        val theBytes = file.toByteArray
        java.nio.ByteBuffer.wrap(theBytes)
      }
      case rcl:ClassLoader => {
        val name = cl.getClass.getName.replaceAll("\\.", "/")+".class"
        val istream = cl.getClass.getResourceAsStream(name)
        if (istream == null) {
          logger.error("Couldn't find stream for class")
          null
        } else {
          val buf = new Array[Byte](1024)
          val os = new java.io.ByteArrayOutputStream(1024)
          var br = istream.read(buf)
          while(br >= 0) {
            os.write(buf,0,br)
            br = istream.read(buf)
          }
          java.nio.ByteBuffer.wrap(os.toByteArray)
        }
      }
    }
  }

  def decodeKey(b:Array[Byte]): GenericData.Record = {
    val decoder = new org.apache.avro.io.BinaryDecoder(new java.io.ByteArrayInputStream(b))
    val reader = new org.apache.avro.generic.GenericDatumReader[GenericData.Record](schema)
    reader.read(null,decoder)
  }

  def decodeKey(b:java.nio.ByteBuffer): GenericData.Record = {
    val decoder = new org.apache.avro.io.BinaryDecoder(new java.io.ByteArrayInputStream(b.array, b.position, b.remaining))
    val reader = new org.apache.avro.generic.GenericDatumReader[GenericData.Record](schema)
    reader.read(null,decoder)
  }

  def put[K <: KeyType, V <: ValueType](key: K, value: V): Unit = {
    val nodes = serversForKey(key)
    val pr = new PutRequest
    pr.namespace = namespace
    pr.key = key.toBytes
    pr.value = value.toBytes
    applyToSet(nodes,(rn)=>{Sync.makeRequest(rn,dest,pr,timeout)},nodes.size)
  }

  def getBytes[K <: KeyType](key: K): java.nio.ByteBuffer = {
    val nodes = serversForKey(key)
    val gr = new GetRequest
    gr.namespace = namespace
    gr.key = key.toBytes
    applyToSet(nodes,
               (rn)=> {
                 Sync.makeRequest(rn,dest,gr,timeout) match {
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
        req.codename = func.getClass.getName
        req.closure = getFunctionCode(func)

        ranges.foreach(r => {
          MessageHandler.sendMessage(r.nodes.first, msg)
        })

        var remaining = ranges.count
        loop {
          reactWithin(timeout) {
            case (_, msg: Message) => {
              val resp = msg.body.asInstanceOf[FlatMapResponse]

              //TODO: Use scala idioms for iteration
              val records = resp.records.iterator()
              while(records.hasNext) {
                val rec = retClass.newInstance.asInstanceOf[RetType]
                rec.parse(records.next)
                partialElements ++= List(rec)
              }

              remaining -= 1
              if(remaining < 0) { // count starts a 0
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

  def size():Int = {
    val sv = new SyncVar[Int]
    val s = new java.util.concurrent.atomic.AtomicInteger()
    val ranges = splitRange(null, null).counted
    actor {
      val id = MessageHandler.registerActor(self)
      val msg = new Message
      val crr = new CountRangeRequest
      msg.src = new java.lang.Long(id)
      msg.dest = dest
      msg.body = crr
      crr.namespace = namespace
      ranges.foreach(r => {
        val kr = new KeyRange
        kr.minKey = r.min
        kr.maxKey = r.max
        crr.range = kr
        MessageHandler.sendMessage(r.nodes.first, msg)
      })
      var remaining = ranges.count
      loop {
        reactWithin(timeout) {
          case (_, msg: Message) => {
            val resp = msg.body.asInstanceOf[Int]
            s.addAndGet(resp)
            remaining -= 1
            if(remaining < 0) { // count starts at 0
              sv.set(s.get)
              MessageHandler.unregisterActor(id)
              exit
            }
          }
          case TIMEOUT => {
            logger.warn("Timeout waiting for count to return")
            sv.set(-1)
            MessageHandler.unregisterActor(id)
            exit
          }
          case m => {
            logger.fatal("Unexpected message: " + m)
            sv.set(-1)
            MessageHandler.unregisterActor(id)
            exit
          }
        }
      }
    }
    sv.get
  }


  def filter(func: (KeyType, ValueType) => Boolean): Iterable[(KeyType,ValueType)] = {
    val result = new SyncVar[List[(KeyType,ValueType)]]
    var list = List[(KeyType,ValueType)]()
    val ranges = splitRange(null, null).counted
    actor {
      val id = MessageHandler.registerActor(self)
      val msg = new Message
      val fr = new FilterRequest
      msg.src = new java.lang.Long(id)
      msg.dest = dest
      msg.body = fr
      fr.namespace = namespace
      fr.keyType = keyClass.getName
      fr.valueType = valueClass.getName
      fr.codename = func.getClass.getName
      fr.code = getFunctionCode(func)
      ranges.foreach(r => {
        MessageHandler.sendMessage(r.nodes.first, msg)
      })
      var remaining = ranges.count
      loop {
        reactWithin(timeout) {
          case (_, msg: Message) => {
            val resp = msg.body.asInstanceOf[RecordSet]
            val recit = resp.records.iterator
            while (recit.hasNext) {
              val rec = recit.next
              val kinst = keyClass.newInstance.asInstanceOf[KeyType]
              val vinst = valueClass.newInstance.asInstanceOf[ValueType]
              kinst.parse(rec.key)
              vinst.parse(rec.value)
              list = list:::List((kinst,vinst))
            }
            remaining -= 1
            if(remaining < 0) { // count starts at 0
              result.set(list)
              MessageHandler.unregisterActor(id)
              exit
            }
          }
          case TIMEOUT => {
            logger.warn("Timeout waiting for filter to return")
            result.set(null)
            MessageHandler.unregisterActor(id)
            exit
          }
          case m => {
            logger.fatal("Unexpected message in filter: " + m)
            result.set(null)
            MessageHandler.unregisterActor(id)
            exit
          }
        }
      }
    }
    val rlist = result.get
    if (rlist == null)
      null
    else
      rlist
  }

  def foldLeft[B <: SpecificRecordBase](z:(B,B))(func: ((B,B),(B,B)) => (B,B))(implicit retType: scala.reflect.Manifest[B]): (B,B) = 
    doFold(z)(func,0)

  def foldRight[B <: SpecificRecordBase](z:(B,B))(func: ((B,B),(B,B)) => (B,B))(implicit retType: scala.reflect.Manifest[B]): (B,B) = 
    doFold(z)(func,1)

  private def doFold[B <: SpecificRecordBase](z:(B,B))(func: ((B,B),(B,B)) => (B,B),dir:Int)(implicit retType: scala.reflect.Manifest[B]): (B,B) = {
    val retClass = retType.erasure
    var list = List[(B,B)]()
    val result = new SyncVar[List[(B,B)]]
    val ranges = splitRange(null, null).counted
    actor {
      val id = MessageHandler.registerActor(self)
      val msg = new Message
      val fr = new FoldRequest
      msg.src = new java.lang.Long(id)
      msg.dest = dest
      msg.body = fr
      fr.namespace = namespace
      fr.keyType = keyClass.getName
      fr.valueType = valueClass.getName
      fr.retType = retClass.getName
      fr.initValueOne = z._1.toBytes
      fr.initValueTwo = z._2.toBytes
      fr.codename = func.getClass.getName
      fr.code = getFunctionCode(func)
      fr.direction = dir // TODO: Change dir to an enum when we support that
      ranges.foreach(r => {
        MessageHandler.sendMessage(r.nodes.first, msg)
      })
      var remaining = ranges.count
      loop {
        reactWithin(timeout) {
          case (_, msg: Message) => {
            val rec = msg.body.asInstanceOf[Record]
            val retk = retClass.newInstance.asInstanceOf[B]
            val retv = retClass.newInstance.asInstanceOf[B]
            retk.parse(rec.key)
            retv.parse(rec.value)
            list ++= List((retk,retv))
            remaining -= 1
            if(remaining < 0) { // count starts at 0
              result.set(list)
              MessageHandler.unregisterActor(id)
              exit
            }
          }
          case TIMEOUT => {
            logger.warn("Timeout waiting for foldLeft to return")
            result.set(null)
            MessageHandler.unregisterActor(id)
            exit
          }
          case m => {
            logger.fatal("Unexpected message in foldLeft: " + m)
            result.set(null)
            MessageHandler.unregisterActor(id)
            exit
          }
        }
      }
    }
    val rlist = result.get
    if (rlist == null)
      z
    else
      rlist.foldLeft(z)(func)
  }

  // call from within an actor only
  private def bulkPutBody(rn:RemoteNode, csr:CopyStartRequest, polServ:polServer, valBytes:Array[Byte], srec:IntRec, erec:IntRec):Unit = {
    val id = MessageHandler.registerActor(self)
    val myId = new java.lang.Long(id)
    Sync.makeRequest(rn,dest,csr,timeout) match {
      case tsr: TransferStartReply => {
        logger.warn("Got TransferStartReply, sending data")
        val buffer = new AvroArray[Record](100, Schema.createArray((new Record).getSchema))
        val sendIt = new SendIter(rn,id,tsr.recvActorId,buffer,100,0,logger)
        var recsSent:Long = 0
        val sirec = new IntRec
        val eirec = new IntRec
        if (polServ.min != null)
          sirec.parse(polServ.min)
        else
          sirec.f1 = srec.f1
        if (sirec.f1 < srec.f1)
          sirec.f1 = srec.f1
        
        if (polServ.max != null) {
          eirec.parse(polServ.max)
          eirec.f1 = eirec.f1 - 1
        }
        else
          eirec.f1 = erec.f1-1
        if (eirec.f1 > erec.f1)
          eirec.f1 = erec.f1-1
        for (i <- (sirec.f1 to eirec.f1)) {
          val rec = new Record
          sirec.f1 = i
          rec.key = sirec.toBytes
          rec.value = valBytes
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
  }

  def bulkPut(start:Int, end:Int, valByteCount:Int): Long = {
    val kinst = keyClass.newInstance.asInstanceOf[KeyType]
    kinst match {
      case ir:IntRec => {} // this one is okay
      case _ => {
        logger.fatal("Can't bulkPut to namespaces that don't use IntRec keys")
        return 0
      }
    }
    val vinst = valueClass.newInstance.asInstanceOf[ValueType]
    val valBytes = 
    vinst match {
      case ir:IntRec => {
        ir.f1 = 42 // the answer
        ir.toBytes
      }
      case sr:StringRec => {
        sr.f1 = "x"*valByteCount
        sr.toBytes
      }
      case _ => {
        logger.fatal("bulkPut only supported to namespaces with IntRec or StringRec values")
        return 0
      }
    }
    val srec = new IntRec
    val erec = new IntRec
    srec.f1 = start
    erec.f1 = end
    val rangeIt = splitRange(srec,erec)
    var c = 0
    val startTime = System.currentTimeMillis()
    while(rangeIt.hasNext()) {
      val polServ = rangeIt.next
      val rng = new KeyRange
      rng.minKey=polServ.min
      rng.maxKey=polServ.max
      val csr = new CopyStartRequest
      csr.ranges = new AvroArray[KeyRange](1024, Schema.createArray((new KeyRange).getSchema))
      csr.ranges.add(rng)
      csr.namespace = namespace
      polServ.nodes.foreach(node => {
        c+=1
        val a = new Actor {
          self.trapExit = true
          def act() {
            bulkPutBody(node,csr,polServ,valBytes,srec,erec)
          }
        }
        link(a)
        a.start
      })
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
