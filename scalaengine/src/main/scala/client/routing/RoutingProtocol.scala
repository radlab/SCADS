package edu.berkeley.cs.scads.storage.routing


import edu.berkeley.cs.scads.util.RangeTable
import edu.berkeley.cs.scads.util.RangeType
import edu.berkeley.cs.scads.storage.AvroSerializing

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



trait RoutingProtocol [KeyType <: IndexedRecord,
                      ValueType <: IndexedRecord,
                      RecordType <: IndexedRecord,
                      RType] {
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
  }

  onDelete {
    val storageHandlers = routingTable.rTable.flatMap(_.values.map(_.storageService)).toSet
    storageHandlers foreach { handler =>
      handler !? DeleteNamespaceRequest(namespace) match {
        case DeleteNamespaceResponse() =>
          logger.info("Successfully deleted namespace %s on StorageHandler %s", namespace, handler)
        case InvalidNamespace(_) =>
          logger.error("Got invalid namespace error for namespace %s on StorageService %s", namespace, handler)
        case e =>
          logger.error("Unexpected message from DeleteNamespaceRequest: %s", e)
      }
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

  def mergePartitions(mergeKeys: Seq[RoutingKeyType]): Unit = {
    val oldPartitions = for(mergeKey <- mergeKeys) yield {
      require(routingTable.checkMergeCondition(mergeKey), "Either the key is not a split key, or the sets are different and can not be merged") //Otherwise we can not merge the partitions

      val bound = routingTable.lowerUpperBound(mergeKey)
      val leftPart = bound.left.values
      val rightPart = bound.center.values //have to use center as this is the partition with the split key
      val storageServers = leftPart.map(_.storageService)
      val mergePartition = createPartitions(bound.left.startKey, bound.right.startKey, storageServers)

      routingTable = routingTable.merge(mergeKey, mergePartition)
      (leftPart, rightPart)
    }

    storeAndPropagateRoutingTable()

    for((leftPart, rightPart) <- oldPartitions){
      deletePartitionService(leftPart)
      deletePartitionService(rightPart)
    }
  }


  protected def deserializeRoutingKey(key: Array[Byte]) : RoutingKeyType ;

  protected def serializeRoutingKey(key : RoutingKeyType) : Array[Byte];


  /**
   * Replicates a partition to the given storageHandler.
   */
  def replicatePartitions(targets : Seq[(PartitionService, StorageService)]): Seq[PartitionService] = {
    val result = for((partitionHandler, storageHandler) <- targets) yield {
      val (startKey, endKey) = getStartEndKey(partitionHandler)
      val newPartition = createPartitions(startKey, endKey, Seq(storageHandler)).head
      routingTable = routingTable.addValueToRange(startKey, newPartition)
      (newPartition, partitionHandler)
    }
    storeAndPropagateRoutingTable()
    for((newPartition, oldPartition) <- result){
      newPartition !? CopyDataRequest(oldPartition, false) match {
        case CopyDataResponse() => ()
        case _ => throw new RuntimeException("Unexpected Message")
      }
    }
    return result.map(_._1)
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

    val keys = partitionHandler !? GetResponsibilityRequest() match {
      case GetResponsibilityResponse(s, e) => (s.map(deserializeRoutingKey(_)), e.map(deserializeRoutingKey(_)))
      case _ => throw new RuntimeException("Unexpected Message")
    }
    return keys
  }



  private def deletePartitionService(partitions: Seq[PartitionService]): Unit = {
    for (partition <- partitions) {
      partition.storageService !? DeletePartitionRequest(partition.partitionId) match {
        case DeletePartitionResponse() => ()
        case _ => throw new RuntimeException("Unexpected Message")
      }
    }
  }


  private def createPartitions(startKey: Option[RoutingKeyType], endKey: Option[RoutingKeyType], servers: Seq[StorageService])
  : Seq[PartitionService] = {
    for (server <- servers) yield {
      server !? CreatePartitionRequest(namespace, startKey.map(serializeRoutingKey(_)), endKey.map(serializeRoutingKey(_))) match {
        case CreatePartitionResponse(partitionActor) => partitionActor
        case _ => throw new RuntimeException("Unexpected Message")
      }
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

  private def loadRoutingTable() = {
    val zkNode = nsRoot.get(ZOOKEEPER_ROUTING_TABLE)
    val rangeSeq = new RoutingTableMessage()
    zkNode match {
      case None => throw new RuntimeException("Can not load empty routing table")
      case Some(a) => rangeSeq.parse(a.data)
    }
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
        a.corresponds(b)((v1, v2) => v1.storageService.id == v2.storageService.id)
      })
  }



}
