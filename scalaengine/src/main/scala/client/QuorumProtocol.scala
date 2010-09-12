package edu.berkeley.cs.scads.storage

import edu.berkeley.cs.scads.comm._
import org.apache.avro.generic.IndexedRecord
import net.lag.logging.Logger
import org.apache.zookeeper.CreateMode
import java.nio.ByteBuffer
import actors.Actor
import java.util.concurrent.TimeUnit
import collection.mutable.{ArrayBuffer, MutableList, HashMap}
import java.util.Arrays

/**
 * Quorum Protocol
 * read and write quorum are defined as percentages (0< X <=1). The sum of read and write has to be greater than 1
 */
abstract class QuorumProtocol[KeyType <: IndexedRecord, ValueType <: IndexedRecord]
(namespace: String,
 timeout: Int,
 root: ZooKeeperProxy#ZooKeeperNode)(implicit cluster: ScadsCluster)
        extends Namespace[KeyType, ValueType](namespace, timeout, root)(cluster) with AvroComparator {
  protected var readQuorum: Double = 0.001
  protected var writeQuorum: Double = 1

  protected val ZK_QUORUM_CONFIG = "quorumProtConfig"

  protected val logger = Logger()

  protected def serversForKey(key: KeyType): List[PartitionService]

  protected def serversForRange(startKey: Option[KeyType], endKey: Option[KeyType]): List[List[PartitionService]]


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
    val sKey = startKey.map(serializeKey(_))
    val eKey = endKey.map(serializeKey(_))
    var rangeRequest = new GetRangeRequest(sKey, eKey, None, None, true)
    var result : List[(PartitionService, List[(KeyType, ValueType)])] = Nil
    for(partitions <- ranges) {
      for(partition <- partitions) {
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

  def get[K <: KeyType](key: K): Option[ValueType] = {
    val (servers, quorum) = readQuorumForKey(key)
    val serKey = serializeKey(key)
    val getRequest = GetRequest(serKey)
    val responses = servers.map(_ !! getRequest)

    val handler = new GetHandler(serKey, responses)
    val record = handler.vote(quorum)
    if (handler.failed)
      throw new RuntimeException("Not enough servers responded. We need to throw better exceptions for that case")
    ReadRepairer ! handler
    return extractValueFromRecord(record)
  }

  def getRange(startKey: Option[KeyType], endKey: Option[KeyType], limit: Option[Int] = None, offset: Option[Int] = None, backwards: Boolean = false): Seq[(KeyType, ValueType)] = {
    val partitions = if (backwards) serversForRange(startKey, endKey).reverse else serversForRange(startKey, endKey)
    var handlers: ArrayBuffer[RangeHandle] = new ArrayBuffer[RangeHandle]
    val sKey = startKey.map(serializeKey(_))
    val eKey = endKey.map(serializeKey(_))
    var rangeRequest = new GetRangeRequest(sKey, eKey, limit, offset, !backwards)
    var serversToRequest: List[List[PartitionService]] = Nil
    if (limit.isEmpty) {
      for (servers <- partitions)
        handlers.append(new RangeHandle(servers.map(_ !! rangeRequest)))
    } else {
      handlers.append(new RangeHandle(partitions.head.map(_ !! rangeRequest)))
      serversToRequest = partitions.tail
    }
    var result = new ArrayBuffer[(KeyType, ValueType)]
    var openRec: Long = if (limit.isDefined) limit.get else java.lang.Long.MAX_VALUE
    var servers = partitions
    var cur = 0
    for (handler <- handlers) {
      if (openRec > 0) {
        val records = handler.vote(readQuorum(servers.head.size))
        if (handler.failed)
          throw new RuntimeException("Not enough servers responded. We need to throw better exceptions for that case")
        //if we do not get enough records, we request more, before we process the current batch
        if (records.length <= openRec && !serversToRequest.isEmpty) {
          if (limit.isDefined)
            rangeRequest = new GetRangeRequest(sKey, eKey, Some(openRec.toInt), offset, !backwards)
          handlers.append(new RangeHandle(servers.head.map(_ !! rangeRequest)))
          servers = servers.tail
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


  def getPrefix[K <: KeyType](key: K, prefixSize: Int, limit: Option[Int] = None, ascending: Boolean = true): Seq[(KeyType, ValueType)] = throw new RuntimeException("Unimplemented")

  def size(): Int = throw new RuntimeException("Unimplemented")

  def ++=(that: Iterable[(KeyType, ValueType)]): Unit = that.foreach(r => put(r._1, r._2))


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

  class GetHandler(val key: Array[Byte], val futures: Seq[MessageFuture], val timeout: Long = 100) {
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

  object ReadRepairer extends Actor {
    this.start
    
    def act() {
      while (true) {
        receive {
          case handler: GetHandler =>   {
            handler.processRest()
            for (server <- handler.repairList)
              server !! PutRequest(handler.key, handler.winnerValue)
          }
          case handler: RangeHandle => {
            handler.processRest()
            for ((key, (data, servers)) <- handler.loosers){
              for (server <- servers)     {

                server !! PutRequest(key, Some(data))
              }
            }
          }
          case _ => throw new RuntimeException("Unknown message")

        }
      }
    }
  }

}
