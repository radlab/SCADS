package edu.berkeley.cs.scads.storage

import scala.collection.JavaConversions._

import edu.berkeley.cs.scads.comm._
import org.apache.avro.generic.{GenericData, GenericRecord, IndexedRecord}
import org.apache.avro.Schema
import Schema.Type
import org.apache.avro.util.Utf8
import net.lag.logging.Logger
import org.apache.zookeeper.CreateMode
import java.nio.ByteBuffer
import actors.Actor
import java.util.concurrent.TimeUnit
import collection.mutable.{ArrayBuffer, MutableList, HashMap}
import java.util.Arrays
import scala.concurrent.ManagedBlocker

private[storage] object QuorumProtocol {
  val MinString = "" 
  val MaxString = new String(Array.fill[Byte](20)(127.asInstanceOf[Byte]))
}

/**
 * Quorum Protocol
 * read and write quorum are defined as percentages (0< X <=1). The sum of read and write has to be greater than 1
 */
abstract class QuorumProtocol[KeyType <: IndexedRecord, ValueType <: IndexedRecord]
(namespace: String,
 timeout: Int,
 root: ZooKeeperProxy#ZooKeeperNode)(implicit cluster: ScadsCluster)
        extends Namespace[KeyType, ValueType](namespace, timeout, root)(cluster) with AvroComparator {
  
  import QuorumProtocol._

  protected var readQuorum: Double = 0.001
  protected var writeQuorum: Double = 1

  protected val ZK_QUORUM_CONFIG = "quorumProtConfig"

  protected val logger = Logger()

  def setReadWriteQuorum(readQuorum: Double, writeQuorum: Double) = {
    require(0 < readQuorum && readQuorum <= 1, "Read quorum has to be in the range 0 < RQ <= 1 but was " + readQuorum)
    require(0 < writeQuorum && writeQuorum <= 1, "Write quorum has to be in the range 0 < WQ <= 1 but was " + writeQuorum)
    require((writeQuorum + readQuorum) >= 1, "Read + write quorum has to be >= 1 but was " + (readQuorum + writeQuorum))
    this.readQuorum = readQuorum
    this.writeQuorum = writeQuorum
    val config = new QuorumProtocolConfig(readQuorum, writeQuorum)
    val zkConfig = nsRoot.get(ZK_QUORUM_CONFIG)
    assert(zkConfig.isDefined)
    zkConfig.get.data = config.toBytes
  }

  override def load(): Unit = {
    super.load()
    val zkConfig = nsRoot.get(ZK_QUORUM_CONFIG)
    assert(zkConfig.isDefined)
    val config = new QuorumProtocolConfig()
    config.parse(zkConfig.get.data)
    readQuorum = config.readQuorum
    writeQuorum = config.writeQuorum
  }

  override def create(ranges: Seq[(Option[KeyType], List[StorageService])]) {
    super.create(ranges)
    val config = new QuorumProtocolConfig(readQuorum, writeQuorum)
    nsRoot.createChild(ZK_QUORUM_CONFIG, config.toBytes, CreateMode.PERSISTENT)
  }

  private def writeQuorumForKey(key: KeyType): (List[PartitionService], Int) = {
    val servers = serversForKey(key)
    (servers, scala.math.ceil(servers.size * writeQuorum).toInt)
  }

  private def readQuorum(nbServers: Int): Int = scala.math.ceil(nbServers * readQuorum).toInt

  private def readQuorumForKey(key: KeyType): (List[PartitionService], Int) = {
    val servers = serversForKey(key)
    (servers, readQuorum(servers.size))
  }

  /**
   * Returns all value versions for a given key. Does not perform read-repair.
   */
  def getAllVersions[K <: KeyType](key: K): List[(PartitionService, Option[ValueType])] = {
    val (partitions, quorum) = readQuorumForKey(key)
    val serKey = serializeKey(key)
    val getRequest = GetRequest(serKey)

    for (partition <- partitions) yield {
      val values = partition !? getRequest match {
        case GetResponse(v) => extractValueFromRecord(v)
        case u => throw new RuntimeException("Unexpected message during get.")
      }
      (partition, values) 
    }
 }

  def getAllRangeVersions(startKey: Option[KeyType], endKey: Option[KeyType]) : List[(PartitionService, List[(KeyType, ValueType)])] = {
    val ranges = serversForRange(startKey, endKey)
    var result : List[(PartitionService, List[(KeyType, ValueType)])] = Nil
    for(range <- ranges) {
      val rangeRequest = new GetRangeRequest(range.startKey.map(serializeKey(_)), range.endKey.map(serializeKey(_)), None, None, true)
      for(partition <- range.values) {
        val values = partition !? rangeRequest  match {
           case GetRangeResponse(v) => v.map(a => (deserializeKey(a.key), extractValueFromRecord(a.value).get))
           case _ => throw new RuntimeException("Unexpected Message")
        }
        result = (partition, values) :: result
      }
    }
    result.reverse
  }

  def put[K <: KeyType, V <: ValueType](key: K, value: Option[V]): Unit = {
    val (servers, quorum) = writeQuorumForKey(key)
    val putRequest = PutRequest(serializeKey(key), value.map(createRecord))
    val responses = serversForKey(key).map(_ !! putRequest)
    responses.blockFor(quorum)
  }

  @inline private def makeGetHandler(key: KeyType) = {
    val (servers, quorum) = readQuorumForKey(key)
    val serKey = serializeKey(key)
    val getRequest = GetRequest(serKey)
    val responses = servers.map(_ !! getRequest)
    (new GetHandler(serKey, responses), quorum)
  }

  @inline private def finishGetHandler(handler: GetHandler, quorum: Int) = {
    val record = handler.vote(quorum)
    if (handler.failed)
      throw new RuntimeException("Not enough servers responded. We need to throw better exceptions for that case")
    ReadRepairer ! handler
    extractValueFromRecord(record)
  }

  def get[K <: KeyType](key: K): Option[ValueType] = {
    val (handler, quorum) = makeGetHandler(key)
    finishGetHandler(handler, quorum)
  }

  class AsyncGetActor[K <: KeyType](key: K, blockingFuture: BlockingFuture[Option[ValueType]]) 
    extends ManagedBlockingActor {
    def act() {
      managedBlock {
        val (handler, quorum) = makeGetHandler(key)
        blockingFuture.finish(finishGetHandler(handler, quorum))
      }
    }
  }

  def asyncGet[K <: KeyType](key: K): ScadsFuture[Option[ValueType]] = {
    val ftch = new BlockingFuture[Option[ValueType]]
    (new AsyncGetActor(key, ftch)).start()
    ftch
  }

  protected def minVal(fieldType: Type, fieldSchema: Schema): Any = fieldType match {
    case Type.BOOLEAN => false
    case Type.DOUBLE => java.lang.Double.MIN_VALUE
    case Type.FLOAT => java.lang.Float.MIN_VALUE
    case Type.INT => java.lang.Integer.MIN_VALUE
    case Type.LONG => java.lang.Long.MIN_VALUE
    case Type.STRING => new Utf8(MinString)
    case Type.RECORD =>
      fillOutKey(newRecordInstance(fieldSchema), () => newRecordInstance(fieldSchema))(minVal _)
    case unsupportedType =>
      throw new RuntimeException("Invalid key type in partial key getRange. " + unsupportedType + " not supported for inquality queries.")
  }

  protected def maxVal(fieldType: Type, fieldSchema: Schema): Any = fieldType match {
    case Type.BOOLEAN => true
    case Type.DOUBLE => java.lang.Double.MAX_VALUE
    case Type.FLOAT => java.lang.Float.MAX_VALUE
    case Type.INT => java.lang.Integer.MAX_VALUE
    case Type.LONG => java.lang.Long.MAX_VALUE
    case Type.STRING => new Utf8(MaxString) 
    case Type.RECORD => 
      fillOutKey(newRecordInstance(fieldSchema), () => newRecordInstance(fieldSchema))(maxVal _)
    case unsupportedType =>
      throw new RuntimeException("Invalid key type in partial key getRange. " + unsupportedType + " not supported for inquality queries.")
  }

  protected def fillOutKey[R <: IndexedRecord](keyPrefix: R, keyFactory: () => R)(fillFunc: (Type, Schema) => Any): R = {
    val filledRec = keyFactory()

    logger.info("Original key: %s", keyPrefix)
    keyPrefix.getSchema.getFields.foreach(field => {
      logger.info("Processing field %s", field)
      if(keyPrefix.get(field.pos) == null)
       filledRec.put(field.pos, fillFunc(field.schema.getType, field.schema))
      else
       filledRec.put(field.pos, keyPrefix.get(field.pos))
    })
    logger.info("Filled key: %s", filledRec)
    filledRec
  }

  //TODO Offset does not work if split over several partitions
  def getRange(startKeyPrefix: Option[KeyType], endKeyPrefix: Option[KeyType], limit: Option[Int] = None, offset: Option[Int] = None, backwards: Boolean = false): Seq[(KeyType, ValueType)] = {
    val startKey = startKeyPrefix.map(prefix => fillOutKey(prefix, newKeyInstance _)(minVal))
    val endKey = endKeyPrefix.map(prefix => fillOutKey(prefix, newKeyInstance _)(maxVal))
    var partitions = if (backwards) serversForRange(startKey, endKey).reverse else serversForRange(startKey, endKey)
    var handlers: ArrayBuffer[RangeHandle] = new ArrayBuffer[RangeHandle]
    val sKey = startKey.map(serializeKey(_))
    val eKey = endKey.map(serializeKey(_))

    do {
      val range = partitions.head
      val rangeRequest = new GetRangeRequest(range.startKey.map(serializeKey(_)), range.endKey.map(serializeKey(_)), limit, offset, !backwards)
      handlers.append(new RangeHandle(range.values.map(_ !! rangeRequest)))
      partitions = partitions.tail
    } while(!partitions.isEmpty &&  limit.isEmpty)

    var result = new ArrayBuffer[(KeyType, ValueType)]
    var openRec: Long = if (limit.isDefined) limit.get else java.lang.Long.MAX_VALUE
    var servers = partitions
    var cur = 0
    for (handler <- handlers) {
      if (openRec > 0) {
        val records = handler.vote(readQuorum(handler.futures.size))
        if (handler.failed)
          throw new RuntimeException("Not enough servers responded. We need to throw better exceptions for that case")
        //if we do not get enough records, we request more, before we process the current batch
        if (records.length <= openRec && !partitions.isEmpty) {
          val range = partitions.head
          val newLimit = if(limit.isDefined) Some(openRec.toInt) else None
          val rangeRequest = new GetRangeRequest(range.startKey.map(serializeKey(_)), range.endKey.map(serializeKey(_)), newLimit, offset, !backwards)
          handlers.append(new RangeHandle(range.values.map(_ !! rangeRequest)))
          partitions = partitions.tail
        }
        result.appendAll(records.flatMap(a =>
          if (openRec > 0) {
            openRec -= 1
            val dValue = extractValueFromRecord(a.value)
            if (dValue.isDefined)
              List((deserializeKey(a.key), dValue.get))
            else
              Nil
          } else
            Nil
          )
        )
      }
    }
    handlers.foreach(ReadRepairer ! _)
    result.toList //If this is to expensive, we should consider a different data structure
  }


  def size(): Int = throw new RuntimeException("Unimplemented")

  private final val BufSize = 1024
  private final val FutureCollSize = 1024

  /**
   * Bulk put. Still obeys write quorums
   */
  def ++=(that: Iterable[(KeyType, ValueType)]) {
    /* Buffers KV tuples until the average buffer size for all the servers is
     * greater than BufSize. Then sends the request to the servers, while
     * repeating the buffering process for the next set of KV tuples. When the
     * tuple stream runs out, or when the outstanding future collection grows
     * above a certain threshold, blocks (via quorum) on each of the returned
     * future collections.
     */
    val serverBuffers = new HashMap[PartitionService, ArrayBuffer[PutRequest]]
    val outstandingFutureColls = new ArrayBuffer[FutureCollection]

    def averageBufferSize =
      serverBuffers.values.foldLeft(0)(_ + _.size) / serverBuffers.size.toDouble 

    def writeServerBuffers() {
      outstandingFutureColls += new FutureCollection(serverBuffers.map { tup => 
        tup._1 !! BulkPutRequest(tup._2.toSeq)
      }.toSeq)
      serverBuffers.clear() // clear buffers
    }

    def blockFutureCollections() {
      outstandingFutureColls.foreach(coll => coll.blockFor(scala.math.ceil(coll.futures.size * writeQuorum).toInt))
      outstandingFutureColls.clear() // clear ftch colls
    }

    that.foreach { kvEntry => 

      val (servers, quorum) = writeQuorumForKey(kvEntry._1)
      val putRequest = PutRequest(serializeKey(kvEntry._1), Some(createRecord(kvEntry._2)))

      for (server <- servers) {
        val buf = serverBuffers.getOrElseUpdate(server, new ArrayBuffer[PutRequest])
        buf += putRequest
      }

      if (averageBufferSize >= BufSize.toDouble)
        // time to flush server buffers
        writeServerBuffers()

      if (outstandingFutureColls.size > FutureCollSize)
        // time to block on collections (via quorum) so that we don't congest
        // the network
        blockFutureCollections()

    }

    if (!serverBuffers.isEmpty) {
      // outstanding keys since last flush - since we've reached the end of
      // the iterable collection, we must write and block now
      writeServerBuffers()
      blockFutureCollections()
    }
  }

  /**
   * Tests the range for conflicts and marks all violations for future resolution.
   * This class is optimized for none/few violations.
   */
  class RangeHandle(val futures: Seq[MessageFuture], val timeout: Long = 100000) {
    val responses = new java.util.concurrent.LinkedBlockingQueue[MessageFuture]
    futures.foreach(_.forward(responses))
    var loosers = new HashMap[Array[Byte], (Array[Byte], List[RemoteActorProxy])]
    var winners = new ArrayBuffer[Record]
    var baseServer: RemoteActorProxy = null //The whole comparison is based on the first response
    var winnerExceptions = new HashMap[Array[Byte], RemoteActorProxy]()
    val startTime = System.currentTimeMillis
    var failed = false
    var ctr = 0


    def vote(quorum: Int): ArrayBuffer[Record] = {
      (1 to quorum).foreach(_ => processNext())
      return winners
    }

    def processRest() = {
      for (i <- 1 to futures.length - ctr) {
        processNext()
      }
    }


    private def processNext(): Unit = {
      ctr += 1
      val time = startTime - System.currentTimeMillis + timeout
      val future = responses.poll(if (time > 0) time else 1, TimeUnit.MILLISECONDS)
      if (future == null) {
        failed = true
        return
      }
      val result = future() match {
        case GetRangeResponse(v) => v
        case GetPrefixResponse(v) => v
        case m => throw new RuntimeException("Unexpected Message (was expecting either GetRangeResponse or GetPrefixResponse): " + m)
      }


      if (winners.isEmpty) {
        //println("first result " + result.map(a => QuorumProtocol.this.deserializeKey(a.key)))
        winners.insertAll(0, result)
        baseServer = future.source.get
      } else {
        merge(result, future.source.get)
      }
    }

    private def merge(newRecords: List[Record], newServer: RemoteActorProxy): Unit = {
      var recordPtr = newRecords
      var i = 0
      while (i < winners.length && !recordPtr.isEmpty) {
        val winnerKey = winners(i).key
        val newKey = recordPtr.head.key
        assert(winners(i).value.isDefined) //should always be defined if we get data back, otherwise delete might not work
        val winnerValue = winners(i).value.get
        assert(recordPtr.head.value.isDefined)
        val newValue = recordPtr.head.value.get
        compare(winnerKey, newKey) match {
          case -1 =>
            i += 1
          case 0 => {
            QuorumProtocol.this.compareRecord(winnerValue, newValue) match {
              case -1 => {
                val outdatedServer = winnerExceptions.getOrElse(winnerKey, baseServer)
                loosers += winnerKey -> (newValue, outdatedServer :: loosers.getOrElse(winnerKey, (null, Nil))._2)
                winnerExceptions += winnerKey -> newServer
                winners.update(i, recordPtr.head)
              }
              case 1 => {
                loosers += winnerKey -> (winnerValue, newServer :: loosers.getOrElse(winnerKey, (null, Nil))._2)
              }
              case 0 => ()
            }
            recordPtr = recordPtr.tail
            i += 1
          }
          case 1 => {
            winners.insert(i, recordPtr.head)
            i += 1 //because of the insert
            winnerExceptions += newKey -> newServer
            recordPtr = recordPtr.tail
          }
        }
      }
    }

  }

  class GetHandler(val key: Array[Byte], val futures: Seq[MessageFuture], val timeout: Long = 5000) {
    val responses = new java.util.concurrent.LinkedBlockingQueue[MessageFuture]
    futures.foreach(_.forward(responses))
    var losers: MutableList[RemoteActorProxy] = new MutableList
    var winners: MutableList[RemoteActorProxy] = new MutableList
    var winnerValue: Option[Array[Byte]] = None
    var ctr = 0
    val startTime = System.currentTimeMillis
    var failed = false

    def repairList() = losers

    def vote(quorum: Int): Option[Array[Byte]] = {
      (1 to quorum).foreach(_ => compareNext())
      return winnerValue
    }

    def processRest() = {
      for (i <- 1 to futures.length - ctr) {
        compareNext()
      }
    }

    private def compareNext(): Unit = {
      ctr += 1
      val time = startTime - System.currentTimeMillis + timeout
      val future = responses.poll(if (time > 0) time else 0, TimeUnit.MILLISECONDS)
      if (future == null) {
        failed = true
        return
      }
      future() match {
        case m@GetResponse(v) => {
          val cmp = QuorumProtocol.this.compareRecord(winnerValue, v)
          if (cmp > 0) {
            future.source.foreach(losers += _)
          } else if (cmp < 0) {
            losers ++= winners
            winners.clear
            future.source.foreach(winners += _)
            winnerValue = v
          }else{
            future.source.foreach(winners += _)
          }
        }
        case _ => throw new RuntimeException("")
      }
    }

  }

  /**
   * ReadRepairer actor - calls to processRest() run in a managed blocker, so that
   * the ForkJoinPool does not starve
   */
  object ReadRepairer extends ManagedBlockingActor {
    start() /* Auto start */
    
    def act() {
      loop {
        react {
          case handler: GetHandler =>
            managedBlock {
              handler.processRest()
            }
            for (server <- handler.repairList)
              server !! PutRequest(handler.key, handler.winnerValue)
          case handler: RangeHandle =>
            managedBlock {
              handler.processRest()
            }
            for ((key, (data, servers)) <- handler.loosers) {
              for (server <- servers) {
                server !! PutRequest(key, Some(data))
              }
            }
          case _ => throw new RuntimeException("Unknown message")
        }
      }
    }
  }

  /**
   * Default ManagedBlocker for a function which has no means to query
   * progress
   */
  class DefaultManagedBlocker(f: () => Unit) extends ManagedBlocker {
    private var completed = false
    def block() = {
      f()
      completed = true
      true
    }
    def isReleasable = completed
  }

  /**
   * Actor which makes it easier to invoke managedBlock
   */
  trait ManagedBlockingActor extends Actor {
    protected def managedBlock(f: => Unit) {
      scheduler.managedBlock(new DefaultManagedBlocker(() => f))
    }
  }

}
