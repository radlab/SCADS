package edu.berkeley.cs.scads.storage.routing


import edu.berkeley.cs.scads.util.{ RangeTable, RangeType }
import edu.berkeley.cs.scads.storage.{ AvroSerializing, ParFuture }

import collection.mutable.HashMap
import org.apache.avro.generic.IndexedRecord
import collection.immutable.{TreeMap, SortedMap}
import org.apache.avro.Schema
import java.util.{Comparator, Arrays}
import edu.berkeley.cs.scads.storage.Namespace
import org.apache.zookeeper.CreateMode
import edu.berkeley.cs.avro.marker.AvroRecord
import edu.berkeley.cs.avro.runtime._
import edu.berkeley.cs.scads.comm._

import java.util.concurrent.TimeUnit


trait RoutingProtocol [KeyType <: IndexedRecord,
                      ValueType <: IndexedRecord,
                      RecordType <: IndexedRecord,
                      RType] extends ParFuture {
  this: Namespace[KeyType, ValueType, RecordType, RType] with AvroSerializing[KeyType, ValueType, RecordType, RType] =>

  def serversForKey(key: KeyType): Seq[PartitionService]

  case class FullRange(startKey: Option[KeyType], endKey: Option[KeyType], values : Seq[PartitionService])
  def serversForRange(startKey: Option[KeyType], endKey: Option[KeyType]): Seq[FullRange]
}



private[storage] object RoutingTableProtocol {
  val ZOOKEEPER_ROUTING_TABLE = "routingtable"
  val ZOOKEEPER_PARTITION_ID = "partitionid"
}

trait RoutingTableProtocol[KeyType <: IndexedRecord,
                      ValueType <: IndexedRecord,
                      RecordType <: IndexedRecord,
                      RType,
                      RoutingKeyType <: IndexedRecord] extends RoutingProtocol[KeyType, ValueType, RecordType, RType]{

  this: Namespace[KeyType, ValueType, RecordType, RType] with AvroSerializing[KeyType, ValueType, RecordType, RType] =>

  import RoutingTableProtocol._


  protected var attemps : Int = 3


  protected var routingTable: RangeTable[RoutingKeyType, PartitionService] = null
  protected var partCtr: Long = 0 //TODO remove when storage service creates IDs

  /**
   *  Creates a new NS with the given servers
   *  The ranges has the form (startKey, servers). The first Key has to be None
   */
  protected def setup(ranges : Seq[(Option[RoutingKeyType], Seq[StorageService])] ) = {
      var rTable: Array[RangeType[RoutingKeyType, PartitionService]] = new Array[RangeType[RoutingKeyType, PartitionService]](ranges.size)
      var startKey: Option[RoutingKeyType] = None
      var endKey: Option[RoutingKeyType] = None
      var i = ranges.size - 1
      nsRoot.createChild("partitions", "".getBytes, CreateMode.PERSISTENT)
      for (range <- ranges.reverse) {
        startKey = range._1
        val handlers = createPartitions(startKey, endKey, range._2)
        rTable(i) = new RangeType[RoutingKeyType, PartitionService](startKey, handlers)
        endKey = startKey
        i -= 1
      }
      createRoutingTable(rTable)
      storeRoutingTable()
      loadRoutingTable()
  }

  onDelete {
    val storageHandlers = routingTable.rTable.flatMap(_.values.map(_.storageService)).toSet
    val delReq = DeleteNamespaceRequest(namespace)
    waitFor(storageHandlers.toSeq.map(h => (h !! delReq, h))) {
      case (DeleteNamespaceResponse(), handler) =>
        logger.info("Successfully deleted namespace %s on StorageHandler %s", namespace, handler)
      case (InvalidNamespace(_), handler) =>
        logger.error("Got invalid namespace error for namespace %s on StorageService %s", namespace, handler)
      case (e, _) =>
        logger.error("Unexpected message from DeleteNamespaceRequest: %s", e)
    }
    routingTable = null
  }

  onLoad {
    loadRoutingTable()
  }

  def serversForKeyImpl(key: RoutingKeyType): Seq[PartitionService] = {
    routingTable.valuesForKey(key)
  }



  def splitPartition(splitPoints: Seq[RoutingKeyType]): Unit = {
    var oldPartitions = for(splitPoint <- splitPoints) yield {
      require(!routingTable.isSplitKey(splitPoint)) //Otherwise it is already a split point
      val bound = routingTable.lowerUpperBound(splitPoint)
      val oldPartitions = bound.center.values
      val storageServers = oldPartitions.map(_.storageService)
      val leftPart = createPartitions(bound.center.startKey, Some(splitPoint), storageServers)
      val rightPart = createPartitions(Some(splitPoint), bound.right.startKey, storageServers)
      routingTable = routingTable.split(splitPoint, leftPart, rightPart)
      oldPartitions
    }

    storeAndPropagateRoutingTable()
    for(oldPartition <- oldPartitions)
      deletePartitionService(oldPartition)
  }

  def mergePartitions(mergeKeys: Seq[RoutingKeyType]): Seq[(Option[RoutingKeyType],Option[RoutingKeyType])] = {
    val mergeBounds = new scala.collection.mutable.ListBuffer[(Option[RoutingKeyType],Option[RoutingKeyType])]()
    val oldPartitions = for(mergeKey <- mergeKeys) yield {
      require(isMergable(mergeKey), "Either the key is not a split key, or the sets are different and can not be merged") //Otherwise we can not merge the partitions

      val bound = routingTable.lowerUpperBound(mergeKey)
      val leftPart = bound.left.values
      val rightPart = bound.center.values //have to use center as this is the partition with the split key
      val storageServers = leftPart.map(_.storageService)
      val mergePartition = createPartitions(bound.left.startKey, bound.right.startKey, storageServers)

      routingTable = routingTable.merge(mergeKey, mergePartition)
      mergeBounds.append((bound.left.startKey,mergeKey))
      (leftPart, rightPart)
    }

    storeAndPropagateRoutingTable()

    for((leftPart, rightPart) <- oldPartitions){
      deletePartitionService(leftPart)
      deletePartitionService(rightPart)
    }
    mergeBounds.toList
  }

  def isMergable(mergeKey:RoutingKeyType):Boolean = {
    routingTable.checkMergeCondition(mergeKey)
  }

  def getMergeKeys(mergeKey:Option[RoutingKeyType]):(Option[RoutingKeyType],Option[RoutingKeyType]) = {
    val bound = routingTable.lowerUpperBound(mergeKey)
    (bound.left.startKey, bound.right.startKey)
  }

  protected def deserializeRoutingKey(key: Array[Byte]) : RoutingKeyType ;

  protected def serializeRoutingKey(key : RoutingKeyType) : Array[Byte];


  /**
   * Replicates a partition to the given storageHandler.
   */
  def replicatePartitions(targets : Seq[(PartitionService, StorageService)]): Seq[PartitionService] = {
    val oldRoutingTable = routingTable
    val result = for((partitionHandler, storageHandler) <- targets) yield {
      val (startKey, endKey) = getStartEndKey(partitionHandler)
      val newPartition = createPartitions(startKey, endKey, Seq(storageHandler)).head
      routingTable = routingTable.addValueToRange(startKey, newPartition)
      (newPartition, partitionHandler)
    }
    storeAndPropagateRoutingTable()
    try {
      waitForAndRetry(
          result.map {
          case (newPartition, oldPartition) => (newPartition, CopyDataRequest(oldPartition, false))
        }, 3*60*1000){
        case CopyDataResponse() => ()
      }
    } catch { case e:RuntimeException => {
      routingTable = oldRoutingTable
      logger.warning("propagting old routing table since replicates didn't succeed")
      storeAndPropagateRoutingTable()
      throw new RuntimeException("Couldn't replicate partitions, reverted to old routing table")
    }}

    result.map(_._1)

  }

  /**
   * This function tests if the routing table is in a good state.
   * Thus, all partitions in the routing table must react and all boundaries must be correct
   */
  def validateRoutingTable(): Boolean = {
    var endKey: Option[RoutingKeyType] = None
    for (range <- routingTable.ranges.reverse) {
      for (partition <- range.values) {
        val keys = getStartEndKey(partition)
        if (keys._1 != range.startKey || keys._2 != endKey) {
          assert(false, "The routing table is corrupted. Partition: " + partition + " with key [" + keys._1 + "," + keys._2 + "] != [" +  range.startKey + "," + endKey + "] from table:" + routingTable)
          return false
        }
      }
      endKey = range.startKey
    }
    return true
  }

  /**
   * This function tests if the whole system is in a valid state
   */
  def validateRoutingSystem(): Boolean = {
    //Retrieve all partitions in the system
    //Check if no partitions are missing
    //Perform validate routing Table
    throw new RuntimeException("Not yet implemented")
  }

  def deletePartitions(partitionHandlers: Seq[PartitionService]): Unit = {
    for(partitionHandler <- partitionHandlers){
      val (startKey, endKey) = getStartEndKey(partitionHandler) //Not really needed, just an additional check
      routingTable = routingTable.removeValueFromRange(startKey, partitionHandler)
    }
    storeAndPropagateRoutingTable()
    deletePartitionService(partitionHandlers)
  }

  def partitions: RangeTable[RoutingKeyType, PartitionService] = routingTable


  //ZooKeeper functions
  def refresh(): Unit = {
    loadRoutingTable()
  }

  def expired(): Unit = {
    //We do nothing.
  }

  private def getStartEndKey(partitionHandler: PartitionService): (Option[RoutingKeyType], Option[RoutingKeyType]) = {
    // This request could be avoided if we would store the ranges in the partitionHandler.
    // However replication is not on the critical path and so expensive anyway that it does not matter
    try {
      val keys = partitionHandler !? GetResponsibilityRequest() match {
        case GetResponsibilityResponse(s, e) => (s.map(deserializeRoutingKey(_)), e.map(deserializeRoutingKey(_)))
        case _ => throw new RuntimeException("Unexpected Message")
      }
      return keys
    } catch {case e:Exception => { logger.warning(e,"issue with GetResponsibilityRequest to %s. rethrowing exception",partitionHandler); throw e }}
  }



  private def deletePartitionService(partitions: Seq[PartitionService]): Unit = {
    waitForAndRetry(partitions.map(p => (p.storageService, DeletePartitionRequest(p.partitionId)))){
      case DeletePartitionResponse() => ()
    }

  }


  private def createPartitions(startKey: Option[RoutingKeyType], endKey: Option[RoutingKeyType], servers: Seq[StorageService])
  : Seq[PartitionService] = {
    val createReq = CreatePartitionRequest(namespace, startKey.map(serializeRoutingKey(_)), endKey.map(serializeRoutingKey(_)))
    waitForAndRetry(servers.map((_, createReq))){
       case CreatePartitionResponse(partitionActor) => partitionActor
    }
  }


  private def storeRoutingTable() = {
    assert(validateRoutingTable(), "Holy shit, we are about to store a crappy Routing Table.")
    val ranges = routingTable.ranges.map(a => KeyRange(a.startKey.map(serializeRoutingKey(_)), a.values))
    val rangeSeq = RoutingTableMessage(ranges)
    val zooKeeperRT = nsRoot.getOrCreate(ZOOKEEPER_ROUTING_TABLE)
    zooKeeperRT.data =  rangeSeq.toBytes
  }

  private def storeAndPropagateRoutingTable() = {
    storeRoutingTable()
    nsRoot.waitUntilPropagated()
  }

  private def loadRoutingTable():Unit = {
		logger.debug("loading routing table")
    val zkNode = nsRoot.get(ZOOKEEPER_ROUTING_TABLE).getOrElse(throw new RuntimeException("Can not load empty routing table"))
    val rangeSeq = new RoutingTableMessage().parse(zkNode.onDataChange(loadRoutingTable))
    val partition = rangeSeq.partitions.map(a => new RangeType(a.startKey.map(deserializeRoutingKey(_)), a.servers))
    createRoutingTable(partition.toArray)
  }

  private def createRoutingTable(partitionHandlers: Seq[PartitionService]): Unit = {
    val arr = new Array[RangeType[RoutingKeyType, PartitionService]](1)
    arr(0) = new RangeType[RoutingKeyType, PartitionService](None, partitionHandlers)
    createRoutingTable(arr)
  }

  /**
   *  Just a helper class to create a table and all comparisons
   */
  private def createRoutingTable(ranges: Array[RangeType[RoutingKeyType, PartitionService]]): Unit = {
    implicit def toRichIndexedRecord[T <: IndexedRecord](i: T) = new RichIndexedRecord[T](i)
    val keySchema: Schema = getKeySchema()
    routingTable = new RangeTable[RoutingKeyType, PartitionService](ranges,
      (a: RoutingKeyType, b: RoutingKeyType) => a.compare(b),
      (a: Seq[PartitionService], b: Seq[PartitionService]) => {
        a.corresponds(b)((v1, v2) => v1.storageService equals v2.storageService)
      })
  }


  def waitForAndRetry[T]( messages: Seq[(RemoteActorProxy,MessageBody)], routingTimeout : Int = 5000, attempts : Int = 3) (f: PartialFunction[MessageBody, T]): Seq[T] = {

    var futures : Seq[( MessageFuture, ((RemoteActorProxy,MessageBody), Int))] = messages.map(message => (message._1 !! message._2, (message, attempts)))

    var triedEverything = false
    while(!triedEverything)  {
      triedEverything = true
      val futuresTmp =
        for(future <- futures) yield {
          //var rFuture = future
          if(future._1.get(routingTimeout, TimeUnit.MILLISECONDS).isEmpty){
            if(future._2._2 > 0) {
              triedEverything = false
              logger.debug("Trying to resend message %s in %s attempts", future._2._1._2, attempts - future._2._2)
              (future._2._1._1 !! future._2._1._2, (future._2._1, future._2._2 - 1))
            }else{
              logger.error("%s attempts to send the message failed. Message: %s", attempts, future._2._1._2)
              throw new RuntimeException("TIMEOUT")
            }
          }else{
            future
          }
      }
      futures = futuresTmp
    }

    /* Make sure all messages can be handled by the given partial function */
    futures.map(_._1()).filterNot(f.isDefinedAt).foreach(msg => throw new RuntimeException("Received unexepected message: %s".format(msg)))
    futures.map(future => f(future._1()))
  }
}
