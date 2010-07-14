package edu.berkeley.cs.scads.storage

import edu.berkeley.cs.scads.comm._

import scala.actors._
import scala.actors.Actor._
import java.util.Arrays

import scala.collection.mutable.HashMap
import scala.concurrent.SyncVar
import scala.tools.nsc.interpreter.AbstractFileClassLoader
import scala.tools.nsc.io.AbstractFile

import org.apache.log4j.Logger

import org.apache.avro.Schema
import org.apache.avro.util.Utf8
import org.apache.avro.generic.GenericData.{Array => AvroArray}
import org.apache.avro.generic.GenericData
import org.apache.avro.io.BinaryData
import org.apache.avro.io.DecoderFactory
import com.googlecode.avro.runtime.ScalaSpecificRecord

import org.apache.zookeeper.CreateMode

/**
 * Handles interaction with a single SCADS Namespace
 * TODO: factor out routing decions into self-contained class
 * TODO: Split into specific/generic record versions with good variance semantics
 * TODO: Add functions for splitting/merging partitions
 * TODO: Handle the need for possible schema resolutions
 */
class Namespace[KeyType <: ScalaSpecificRecord, ValueType <: ScalaSpecificRecord](namespace:String, timeout:Int, root: ZooKeeperProxy#ZooKeeperNode)(implicit keyType: scala.reflect.Manifest[KeyType], valueType: scala.reflect.Manifest[ValueType]) {
  private val dest = ActorName("Storage")
  private val logger = Logger.getLogger("Namespace")
  val keyClass = keyType.erasure.asInstanceOf[Class[ScalaSpecificRecord]]
  val valueClass = valueType.erasure.asInstanceOf[Class[ScalaSpecificRecord]]
  private val nsNode = root.get("namespaces/"+namespace)
  private val schema = Schema.parse(new String(nsNode.get("keySchema").data))

  lazy val keySchema = keyClass.newInstance.asInstanceOf[ScalaSpecificRecord].getSchema()
  lazy val valueSchema = valueClass.newInstance.asInstanceOf[ScalaSpecificRecord].getSchema()
  val partitions = new PartitionPolicy(schema, keyClass, nsNode)

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

  /* send messages to multiple nodes.  nmsgl is a list of a list of nodes and message body.
   * for each element of the list, each node in the list of nodes will have the associated
   * message sent to it.  for each element of nmsgl repliesRequired replies will be waited for
   * before returning
   * */
  private def multiSetApply(nmsgl:List[(List[RemoteNode],MessageBody)], repliesRequired:Int):Array[List[Object]] = {
    val repArray = new Array[List[Object]](nmsgl.length)
    if (nmsgl.length == 0) {
      logger.warn("Empty list passed to multiSetApply, returning an empty list")
      return repArray
    }
    val reps = new Array[Int](nmsgl.length)
    var totalNeeded = nmsgl.length
    val resp = new SyncVar[Array[List[Object]]]
    actor {
      val id = MessageHandler.registerActor(self)
      val msg = new Message
      msg.dest = dest
      msg.src = ActorNumber(id)
      var p = 0L
      nmsgl.foreach((nb)=>{
        msg.body = nb._2
        msg.id = Some(p)
        nb._1.foreach((rn)=> {
          MessageHandler.sendMessage(rn,msg)
        })
        p = p + 1
      })
      loop {
        reactWithin(timeout) {
          case (RemoteNode(hostname, port), reply: Message) => reply.body match {
            case exp: ProcessingException => {
              logger.error("ProcessingException in multiSetApply: "+exp)
              resp.set(repArray)
              MessageHandler.unregisterActor(id)
              exit
            }
					  case obj => {
              val intid = reply.id match {
                case Some(i) => i.intValue
                case None => throw new RuntimeException("No sequence id on message")
              }
              if (reps(intid) < repliesRequired) {
                if (repArray(intid) == null)
                  repArray(intid) = List[Object]()
                repArray(intid) = obj :: repArray(intid)
              }
              reps(intid) += 1
              if (reps(intid) == repliesRequired)
                totalNeeded -= 1
              if (totalNeeded == 0) {
                resp.set(repArray)
                MessageHandler.unregisterActor(id)
                exit
              }
            }
				  }
          case TIMEOUT => {
            logger.error("TIMEOUT in multiSetApply")
            MessageHandler.unregisterActor(id)
            resp.set(repArray)
            exit
          }
          case msg => {
            logger.warn("Unexpected message in multiSetApply: " + msg)
          }
			  }
      }
    }
    resp.get
  }

  /* get array of bytes which is the compiled version of cl */
  private def getFunctionCode(cl:AnyRef):Array[Byte] = {
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
        file.toByteArray
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
          os.toByteArray
        }
      }
    }
  }

  def decodeKey(b:Array[Byte]): GenericData.Record = {
    val decoder = DecoderFactory.defaultFactory().createBinaryDecoder(b, null)
    val reader = new org.apache.avro.generic.GenericDatumReader[GenericData.Record](schema)
    reader.read(null,decoder)
  }

  def put[K <: KeyType, V <: ValueType](key: K, value: V): Unit = put(key, Some(value))
  def put[K <: KeyType, V <: ValueType](key: K, value: Option[V]): Unit = {
    val nodes = partitions.serversForKey(key)
    val pr = PutRequest(namespace, key.toBytes, value match {case Some(r) => Some(r.toBytes); case None => None})
    if (nodes.length <= 0) {
      logger.warn("No nodes responsible for this key, not doing anything")
      return
    }
    applyToSet(nodes,(rn)=>{Sync.makeRequest(rn,dest,pr,timeout)},nodes.size)
  }

  def getBytes[K <: KeyType](key: K): Array[Byte] = {
    val nodes = partitions.serversForKey(key)
    if (nodes.length <= 0) {
      logger.warn("No node responsible for this key, returning null")
      return null
    }
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
      case bb:Array[Byte] => bb
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

  def minRecord(rec:ScalaSpecificRecord, prefix:Int, ascending:Boolean):Unit = {
    val fields = rec.getSchema.getFields
    for (i <- (prefix to (fields.size() - 1))) { // set remaining values to min/max
      fields.get(i).schema.getType match {
        case org.apache.avro.Schema.Type.ARRAY =>
          if (ascending)
            rec.put(i,new GenericData.Array(0,fields.get(i).schema))
          else
            throw new Exception("Can't do descending search with an array in the prefix")
        case org.apache.avro.Schema.Type.BOOLEAN =>
          if (ascending)
            rec.put(i,false)
          else
            rec.put(i,true)
        case org.apache.avro.Schema.Type.BYTES =>
          if (ascending)
            rec.put(i,"".getBytes)
          else
            throw new Exception("Can't do descending search with bytes the prefix")
        case org.apache.avro.Schema.Type.DOUBLE =>
          if (ascending)
            rec.put(i,java.lang.Double.MIN_VALUE)
          else
            rec.put(i,java.lang.Double.MAX_VALUE)
        case org.apache.avro.Schema.Type.ENUM =>
          throw new Exception("ENUM not supported at the moment")
        case org.apache.avro.Schema.Type.FIXED =>
          throw new Exception("FIXED not supported at the moment")
        case org.apache.avro.Schema.Type.FLOAT =>
          if (ascending)
            rec.put(i,java.lang.Float.MIN_VALUE)
          else
            rec.put(i,java.lang.Float.MAX_VALUE)
        case org.apache.avro.Schema.Type.INT =>
          if (ascending)
            rec.put(i,java.lang.Integer.MIN_VALUE)
          else
            rec.put(i,java.lang.Integer.MAX_VALUE)
        case org.apache.avro.Schema.Type.LONG =>
          if (ascending)
            rec.put(i,java.lang.Long.MIN_VALUE)
          else
            rec.put(i,java.lang.Long.MAX_VALUE)
        case org.apache.avro.Schema.Type.MAP =>
          throw new Exception("MAP not supported at the moment")
        case org.apache.avro.Schema.Type.NULL =>
          // null is only null, so it's already min, has no max
          if (!ascending)
            throw new Exception("Can't do descending search with null in the prefix")
        case org.apache.avro.Schema.Type.RECORD =>
          if (rec.get(i) != null)
            minRecord(rec.get(i).asInstanceOf[ScalaSpecificRecord],0,ascending)
        case org.apache.avro.Schema.Type.STRING =>
          if (ascending)
            rec.put(i,new Utf8(""))
          else {
            // NOTE: We make the "max" string 20 max char values.  This won't work if you're putting big, max valued strings in your db
            rec.put(i,new Utf8(new String(Array.fill[Byte](20)(127.asInstanceOf[Byte]))))
          }
        case org.apache.avro.Schema.Type.UNION =>
          throw new Exception("UNION not supported at the moment")
        case other =>
          logger.warn("Got a type I don't know how to set to minimum, this getPrefix might not behave as expected: "+other)
      }
    }
  }

  def getPrefix[K <: KeyType](key: K, fields: Int, limit: Int = -1, ascending: Boolean = true):Seq[(KeyType,ValueType)] = {
    val nodes = partitions.serversForKey(key)
    val gpr = new GetPrefixRequest
    gpr.namespace = namespace
    if (limit >= 0)
      gpr.limit = Some(limit)
    gpr.ascending = ascending

    val fcount = key.getSchema.getFields.size
    if (fields > fcount)
      throw new Throwable("Request fields larger than number of fields key has")

    minRecord(key,fields,ascending)

    gpr.start = key.toBytes
    gpr.fields = fields
    var retList = List[(KeyType,ValueType)]()
    applyToSet(nodes,
               (rn)=> {
                 Sync.makeRequest(rn,dest,gpr,timeout)
               },1) match {
      case rs:RecordSet => {
        val recit = rs.records.iterator
        while (recit.hasNext) {
          val rec = recit.next
          val kinst = keyClass.newInstance.asInstanceOf[KeyType]
          val vinst = valueClass.newInstance.asInstanceOf[ValueType]
          kinst.parse(rec.key)
          vinst.parse(rec.value)
          retList = (kinst,vinst) :: retList
        }
      }
      case other => {
        logger.error("Invalid return from getPrefix request: "+other)
      }
    }
    retList.reverse
  }


  def getRange[K <: KeyType](start:K, end:K):Seq[(KeyType,ValueType)] =
    getRange(start,end,0,0,false)
  def getRange[K <: KeyType](start:K, end:K, limit:Int, offset: Int, backwards:Boolean): Seq[(KeyType,ValueType)] = {
    val nodeIter = partitions.splitRange(start,end)
    var nol = List[(List[RemoteNode],MessageBody)]()
    while(nodeIter.hasNext()) {
      val polServ = nodeIter.next
      val kr = new KeyRange(
        if (start != null || polServ.min != null) {
          if (start == null)
            polServ.min
          else if (polServ.min == null)
            Some(start.toBytes)
          else if (BinaryData.compare(start.toBytes,0,polServ.min.get,0,schema) < 0) // start key is less
            polServ.min
          else
            Some(start.toBytes)
        } else null,
        if (end != null || polServ.max != null) {
          if (end == null)
            polServ.max
          else if (polServ.max == null)
            Some(end.toBytes)
          else if (BinaryData.compare(end.toBytes,0,polServ.max.get,0,schema) < 0) // end key is less
            Some(end.toBytes)
          else
            polServ.max
        } else null,
        if (limit != 0)
          Some(limit)
        else
          None,
        Some(offset),backwards)
      val grr = new GetRangeRequest
      grr.namespace = namespace
      grr.range = kr
      nol = (polServ.nodes,grr) :: nol
    }
    val r = multiSetApply(nol.reverse,1) // we'll just take the first reply from each set
    var retList = List[(KeyType,ValueType)]()
    var added = 0
    val reps =
      if (backwards)
        r.reverse
      else
        r
    reps.foreach((repl) => {
      if (limit == 0 || (added < limit)) {
        val rep = repl(0)
        val rs = rep.asInstanceOf[RecordSet]
        val recit = rs.records.iterator
        while (recit.hasNext && (limit == 0 || (added < limit))) {
          val rec = recit.next
          val kinst = keyClass.newInstance.asInstanceOf[KeyType]
          val vinst = valueClass.newInstance.asInstanceOf[ValueType]
          kinst.parse(rec.key)
          vinst.parse(rec.value)
          retList = (kinst,vinst) :: retList
          added += 1
        }
      }
    })
    retList.reverse
  }

  def flatMap[RetType <: ScalaSpecificRecord](func: (KeyType, ValueType) => List[RetType])(implicit retType: scala.reflect.Manifest[RetType]): Seq[RetType] = {
    class ResultSeq extends Actor with Seq[RetType] {
      val retClass = retType.erasure
      val result = new SyncVar[List[RetType]]

      def apply(ordinal: Int): RetType = result.get.apply(ordinal)
      def length: Int = result.get.length
      override def elements: Iterator[RetType] = result.get.iterator
      override def iterator: Iterator[RetType] = elements

      def act(): Unit = {
        val id = MessageHandler.registerActor(self)
        val ranges = partitions.splitRange(null, null)
        var remaining = 0 /** Use ranges.size once we upgrade to RC2 or greater */
        var partialElements = List[RetType]()
        //val msg = new Message
        //val req = new FlatMapRequest
        //msg.src = new java.lang.Long(id)
        //msg.dest = dest
        //msg.body = req
        //req.namespace = namespace
        //req.keyType = keyClass.getName
        //req.valueType = valueClass.getName
        //req.codename = func.getClass.getName
        //req.closure = getFunctionCode(func)

        val msg = Message(
                ActorNumber(id),
                dest,
                null,
                FlatMapRequest(
                    namespace,
                    keyClass.getName,
                    valueClass.getName,
                    func.getClass.getName,
                    getFunctionCode(func)))

        ranges.foreach(r => {
          MessageHandler.sendMessage(r.nodes.head, msg)
          remaining += 1
        })

        loop {
          reactWithin(timeout) {
            case (_, msg: Message) => {
              val resp = msg.body.asInstanceOf[FlatMapResponse]

              //TODO: Use scala idioms for iteration
              //val records = resp.records.iterator()
              //while(records.hasNext) {
              //  val rec = retClass.newInstance.asInstanceOf[RetType]
              //  rec.parse(records.next)
              //  partialElements ++= List(rec)
              //}
              resp.records.foreach( b => {
                  val rec = retClass.newInstance.asInstanceOf[RetType]
                  rec.parse(b)
                  partialElements ++= List(rec)
              })


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
    val ranges = partitions.splitRange(null, null)
    actor {
      val id = MessageHandler.registerActor(self)
      val msg = new Message
      val crr = new CountRangeRequest
      //msg.src = new java.lang.Long(id)
      msg.src = ActorNumber(id)
      msg.dest = dest
      msg.body = crr
      crr.namespace = namespace
      var remaining = 0 /** use ranges.size once we upgrade to RC2 */
      ranges.foreach(r => {
        val kr = new KeyRange
        kr.minKey = r.min
        kr.maxKey = r.max
        crr.range = kr
        MessageHandler.sendMessage(r.nodes.head, msg)
        remaining += 1
      })

      loop {
        reactWithin(timeout) {
          case (_, msg: Message) => {
            val resp = msg.body.asInstanceOf[Int]
            s.addAndGet(resp)
            remaining -= 1
            if(remaining <= 0) {
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
    val ranges = partitions.splitRange(null, null)
    actor {
      val id = MessageHandler.registerActor(self)
      val msg = new Message
      val fr = new FilterRequest
      //msg.src = new java.lang.Long(id)
      msg.src = ActorNumber(id)
      msg.dest = dest
      msg.body = fr
      fr.namespace = namespace
      fr.keyType = keyClass.getName
      fr.valueType = valueClass.getName
      fr.codename = func.getClass.getName
      fr.code = getFunctionCode(func)
      var remaining = 0 /** use ranges.size once we upgrade to RC2 */
      ranges.foreach(r => {
        MessageHandler.sendMessage(r.nodes.head, msg)
        remaining += 1
      })

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
            if(remaining <= 0) {
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

  def foldLeft(z:(KeyType,ValueType))(func: ((KeyType,ValueType),(KeyType,ValueType)) => (KeyType,ValueType)): (KeyType,ValueType) =
    doFold(z)(func,0)

  def foldRight(z:(KeyType,ValueType))(func: ((KeyType,ValueType),(KeyType,ValueType)) => (KeyType,ValueType)): (KeyType,ValueType) =
    doFold(z)(func,1)

  private def doFold(z:(KeyType,ValueType))(func: ((KeyType,ValueType),(KeyType,ValueType)) => (KeyType,ValueType),dir:Int): (KeyType,ValueType) = {
    var list = List[(KeyType,ValueType)]()
    val result = new SyncVar[List[(KeyType,ValueType)]]
    val ranges = partitions.splitRange(null, null)
    actor {
      val id = MessageHandler.registerActor(self)
      val msg = new Message
      val fr = new FoldRequest
      msg.src = ActorNumber(id)
      msg.dest = dest
      msg.body = fr
      fr.namespace = namespace
      fr.keyType = keyClass.getName
      fr.valueType = valueClass.getName
      fr.initValueOne = z._1.toBytes
      fr.initValueTwo = z._2.toBytes
      fr.codename = func.getClass.getName
      fr.code = getFunctionCode(func)
      fr.direction = dir // TODO: Change dir to an enum when we support that
      var remaining = 0 /** use ranges.size once we upgrade to RC2 */
      ranges.foreach(r => {
        MessageHandler.sendMessage(r.nodes.head, msg)
        remaining += 1
      })

      loop {
        reactWithin(timeout) {
          case (_, msg: Message) => {
            val rec = msg.body.asInstanceOf[Record]
            val retk = keyClass.newInstance.asInstanceOf[KeyType]
            val retv = valueClass.newInstance.asInstanceOf[ValueType]
            retk.parse(rec.key)
            retv.parse(rec.value)
            list ++= List((retk,retv))
            remaining -= 1
            if(remaining <= 0) {
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
      if (dir == 0)
        rlist.foldLeft(z)(func)
      else
        rlist.foldRight(z)(func)
  }

  def foldLeft2L[TypeRemote <: ScalaSpecificRecord,TypeLocal](remInit:TypeRemote,localInit:TypeLocal)
                                             (remFunc: (TypeRemote,(KeyType,ValueType)) => TypeRemote,
                                              localFunc: (TypeLocal,TypeRemote) => TypeLocal): TypeLocal = {
    var list = List[TypeRemote]()
    val result = new SyncVar[List[TypeRemote]]
    val ranges = partitions.splitRange(null, null)
    actor {
      val id = MessageHandler.registerActor(self)
      var remaining = 0 /** use ranges.size once we upgrade to RC2 */
      try {
        val msg = new Message
        val fr = new FoldRequest2L
        msg.src = ActorNumber(id)
        msg.dest = dest
        msg.body = fr
        fr.namespace = namespace
        fr.keyType = keyClass.getName
        fr.valueType = valueClass.getName
        fr.initType = remInit.getClass.getName
        fr.initValue = remInit.toBytes
        fr.codename = remFunc.getClass.getName
        fr.code = getFunctionCode(remFunc)
        fr.direction = 0 // TODO: Change dir to an enum when we support that
        ranges.foreach(r => {
          MessageHandler.sendMessage(r.nodes.head, msg)
          remaining += 1
        })
      } catch {
        case t:Throwable => {
          println("Some error processing foldLeft2L")
          t.printStackTrace()
          result.set(null)
          MessageHandler.unregisterActor(id)
          exit
        }
      }

      loop {
        reactWithin(timeout) {
          case (_, msg: Message) => {
            val rep = msg.body.asInstanceOf[Fold2Reply]
            val repv = remInit.getClass.newInstance.asInstanceOf[TypeRemote]
            repv.parse(rep.reply)
            list = repv :: list
            remaining -= 1
            if(remaining <= 0) {
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
      localInit
    else
      rlist.foldLeft(localInit)(localFunc)
  }

  // TODO: Fix to no longer depend on ordinals from placement policy internals
  def ++=(that:Iterable[(KeyType,ValueType)]): Unit = {
  /*  val limit = 256
    val ret = new SyncVar[Long]
    actor {
      val id = new ActorNumber(MessageHandler.registerActor(self))
      val bufferMap = new HashMap[Int,(scala.collection.mutable.ArrayBuffer[Record],Int)]
      val recvIterMap = new HashMap[RemoteNode, ActorId]
      val start = System.currentTimeMillis
      var cnt = 0
      that foreach (pair => {
        cnt+=1
        val nodes = partitions.serversForKey(pair._1)
        if (true) { //bufferMap.contains(idx)) { // already have an iter open
          val bufseq: (scala.collection.mutable.ArrayBuffer[Record],Int) = null //bufferMap(idx)
          val buf = bufseq._1
          val rec = new Record
          rec.key = pair._1.toBytes
          rec.value = pair._2.toBytes
          buf.append(rec)
          if (buf.size >= limit) { // send the buffer
            val bd = new BulkData
            bd.seqNum = bufseq._2
            //bufseq._2 += 1
            bd.sendActorId = id
            val rs = new RecordSet
            rs.records = buf.toList
            bd.records = rs
            val msg = new Message
            msg.src = id
            msg.body = bd
            nodes.foreach((rn) => {
              if (recvIterMap.contains(rn)) {
                msg.dest = recvIterMap(rn)
                MessageHandler.sendMessage(rn,msg)
              } else {
                logger.warn("Not sending to: "+rn+". Have no iter open to it")
              }
            })
            buf.clear
          }
        } else {
          // validate that we don't have a gap
          if (true) {//partitions.keyComp(nodeCache(idx).min,Some(pair._1.toBytes)) > 0) {
            logger.warn("Gap in partitions, returning empty server list")
            Nil
          } else {
            val rng = new KeyRange
            //rng.minKey=polServ.min
            //rng.maxKey=polServ.max
            val csr = new CopyStartRequest
            csr.ranges = List(rng)
            csr.namespace = namespace
            val msg = new Message
            msg.src = id
            msg.dest = dest
            msg.body = csr
            nodes.foreach((rn) => {
              // we need to open up send iters to all the target nodes
              MessageHandler.sendMessage(rn,msg)
            })
            var repsNeeded = nodes.length
            while (repsNeeded > 0) {
              receiveWithin(timeout) {
                case (rn:RemoteNode, msg:Message) => msg.body match {
                  case tsr:TransferStartReply => {
                    val thenode =
                      if (rn.hostname == "localhost")
                        new RemoteNode(java.net.InetAddress.getLocalHost.getHostName,rn.port)
                      else
                        rn
                    logger.warn("Got tsr from: "+thenode)
                    recvIterMap += thenode -> tsr.recvActorId
                    repsNeeded -= 1
                  }
                  case bda:BulkDataAck => {
                    logger.debug("Data ack, ignoring for now")
                  }
                  case other => {
                    logger.error("Unexpected reply to transfer start request from: "+rn+": "+other)
                    repsNeeded -= 1
                  }
                }
                case TIMEOUT => {
                  logger.error("TIMEOUT waiting for transfer start reply, dieing")
                  exit
                }
                case msg => {
                  logger.error("Unexpected reply to copy start request: "+msg)
                  repsNeeded -= 1
                }
              }
            }
            // okay now we should have a full list of iters for all nodes
            val buffer = new scala.collection.mutable.ArrayBuffer[Record]
            //bufferMap += ((idx, (buffer,0)))
            val rec = new Record
            rec.key = pair._1.toBytes
            rec.value = pair._2.toBytes
          }
        }
      })
      bufferMap.foreach((idxbuf) => {
        val bufseq = idxbuf._2
        val buf = bufseq._1
        val polServ =  nodeCache(idxbuf._1)
        val nodes = polServ.nodes
        if (buf.size >= 0) { // send the buffer
          val bd = new BulkData
          bd.seqNum = bufseq._2
          //bufseq._2 += 1
          bd.sendActorId = id
          val rs = new RecordSet
          rs.records = buf.toList
          bd.records = rs
          val msg = new Message
          msg.src = id
          msg.body = bd
          nodes.foreach((rn) => {
            if (recvIterMap.contains(rn)) {
              msg.dest = recvIterMap(rn)
              MessageHandler.sendMessage(rn,msg)
              } else {
                logger.warn("Not sending to: "+rn+". Have no iter open to it")
              }
          })
          buf.clear
        }
      })
      // now send close messages and wait for everything to close up
      val fin = new TransferFinished
      fin.sendActorId = id
      val msg = new Message
      msg.src = id
      msg.body = fin
      var repsNeeded = 0
      recvIterMap.foreach((rnid)=> {
        msg.dest = rnid._2
        repsNeeded += 1
        MessageHandler.sendMessage(rnid._1,msg)
      })
      while (repsNeeded > 0) {
        receiveWithin(timeout) {
          case (rn:RemoteNode, msg:Message) => msg.body match {
            case bda:BulkDataAck => {
              logger.debug("Data ack, ignoring for now")
            }
            case ric:RecvIterClose => {
              logger.warn("Got close for: "+rn)
              repsNeeded -= 1
            }
            case other => {
              logger.error("Unexpected message type waiting for acks/closes: "+rn+": "+other)
              repsNeeded -= 1
            }
          }
          case TIMEOUT => {
            logger.error("TIMEOUT waiting for transfer start reply, dieing")
            repsNeeded = 0
          }
          case msg => {
            logger.error("Unexpected message waiting for acks/closes: "+msg)
            repsNeeded -= 1
          }
        }
      }
      MessageHandler.unregisterActor(id.num)
      println("Send a total of: "+cnt)
      ret.set(System.currentTimeMillis-start)
    }
    ret.get */

  }
}
