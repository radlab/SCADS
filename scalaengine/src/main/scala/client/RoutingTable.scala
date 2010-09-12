package edu.berkeley.cs.scads.storage.routing


import edu.berkeley.cs.scads.util.RangeTable
import edu.berkeley.cs.scads.util.RangeType

import collection.mutable.HashMap
import org.apache.avro.generic.IndexedRecord
import collection.immutable.{TreeMap, SortedMap}
import org.apache.avro.Schema
import java.util.{Comparator, Arrays}
import edu.berkeley.cs.scads.storage.Namespace
import org.apache.zookeeper.CreateMode
import com.googlecode.avro.marker.AvroRecord
import com.googlecode.avro.runtime._
import edu.berkeley.cs.scads.comm._
/* TODO: Stack RepartitioningProtocol on Routing Table to build working implementation
*  TODO: Change implementation to StartKey -> makes it more compliant to the rest
* */
//abstract trait RepartitioningProtocol[KeyType <: IndexedRecord] extends RoutingTable[KeyType] {
//  override def splitPartition(splitPoint: KeyType): List[PartitionService] = throw new RuntimeException("Unimplemented")
//
//  override def mergePartitions(mergeKey: KeyType): Unit = throw new RuntimeException("Unimplemented")
//
//  override def replicatePartition(splitPoint: KeyType, storageHandler: StorageService): PartitionService = throw new RuntimeException("Unimplemented")
//
//  override def deleteReplica(splitPoint: KeyType, partitionHandler: PartitionService): Unit = throw new RuntimeException("Unimplemented")
//}


abstract trait RoutingProtocol[KeyType <: IndexedRecord, ValueType <: IndexedRecord] extends Namespace[KeyType, ValueType] {
  var routingTable: RangeTable[KeyType, PartitionService] = null
  val ZOOKEEPER_ROUTING_TABLE = "routingtable"
  val ZOOKEEPER_PARTITION_ID = "partitionid"
  var partCtr: Long = 0 //TODO remove when storage service creates IDs

  /**
   *  Creates a new NS with the given servers
   *  The ranges has the form (startKey, servers). The first Key has to be None
   */
  override def create(ranges: Seq[(Option[KeyType], List[StorageService])]) {
    super.create(ranges)
    var rTable: Array[RangeType[KeyType, PartitionService]] = new Array[RangeType[KeyType, PartitionService]](ranges.size)
    var startKey: Option[KeyType] = None
    var endKey: Option[KeyType] = None
    var i = ranges.size - 1
    nsRoot.createChild("partitions", "".getBytes, CreateMode.PERSISTENT)
    for (range <- ranges.reverse) {
      startKey = range._1
      val handlers = createPartitions(startKey, endKey, range._2)
      rTable(i) = new RangeType[KeyType, PartitionService](startKey, handlers)
      endKey = startKey
      i -= 1
    }
    createRoutingTable(rTable)
    storeRoutingTable()
  }

  override def load(): Unit = {
    super.load()
    loadRoutingTable()
  }

  def serversForKey(key: KeyType): List[PartitionService] = {
    routingTable.valuesForKey(key)
  }

  def serversForRange(startKey: Option[KeyType], endKey: Option[KeyType]): List[FullRange] = {
    var ranges = routingTable.valuesForRange(startKey, endKey)
    val result = new  Array[FullRange](ranges.size)
    var sKey: Option[KeyType] = None
    var eKey: Option[KeyType] = None
    for (i <- ranges.size - 1 to 0 by -1){
      sKey = ranges(i).startKey
      result(i) = new FullRange(sKey, eKey, ranges(i).values)
      eKey = sKey
    }
    result.toList
  }

  def splitPartition(splitPoints: List[KeyType]): Unit = {
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

  def mergePartitions(mergeKeys: List[KeyType]): Unit = {
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


  /**
   * Replicates a partition to the given storageHandler.
   */
  def replicatePartitions(targets : List[(PartitionService, StorageService)]): List[PartitionService] = {
    val result = for((partitionHandler, storageHandler) <- targets) yield {
      val (startKey, endKey) = getStartEndKey(partitionHandler)
      val newPartition = createPartitions(startKey, endKey, List(storageHandler)).head
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
    var endKey: Option[KeyType] = None
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




  def deletePartitions(partitionHandlers: List[PartitionService]): Unit = {
    for(partitionHandler <- partitionHandlers){
      val (startKey, endKey) = getStartEndKey(partitionHandler) //Not really needed, just an additional check
      routingTable = routingTable.removeValueFromRange(startKey, partitionHandler)
    }
    storeAndPropagateRoutingTable()
    deletePartitionService(partitionHandlers)
  }

  def partitions: RangeTable[KeyType, PartitionService] = routingTable


  //ZooKeeper functions
  def refresh(): Unit = {
    loadRoutingTable()
  }

  def expired(): Unit = {
    //We do nothing.
  }

  private def getStartEndKey(partitionHandler: PartitionService): (Option[KeyType], Option[KeyType]) = {
    // This request could be avoided if we would store the ranges in the partitionHandler.
    // However replication is not on the critical path and so expensive anyway that it does not matter

    val keys = partitionHandler !? GetResponsibilityRequest() match {
      case GetResponsibilityResponse(s, e) => (s.map(deserializeKey(_)), e.map(deserializeKey(_)))
      case _ => throw new RuntimeException("Unexpected Message")
    }
    return keys
  }



  private def deletePartitionService(partitions: List[PartitionService]): Unit = {
    for (partition <- partitions) {
      partition.storageService !? DeletePartitionRequest(partition.partitionId) match {
        case DeletePartitionResponse() => ()
        case _ => throw new RuntimeException("Unexpected Message")
      }
    }
  }


  private def createPartitions(startKey: Option[KeyType], endKey: Option[KeyType], servers: List[StorageService])
  : List[PartitionService] = {
    for (server <- servers) yield {
      server !? CreatePartitionRequest(namespace, startKey.map(serializeKey(_)), endKey.map(serializeKey(_))) match {
        case CreatePartitionResponse(partitionActor) => partitionActor
        case _ => throw new RuntimeException("Unexpected Message")
      }
    }
  }


  private def storeRoutingTable() = {
    assert(validateRoutingTable(), "Holy shit, we are about to store a crappy Routing Table.")
    val ranges = routingTable.ranges.map(a => KeyRange(a.startKey.map(serializeKey(_)), a.values))
    val rangeList = RoutingTableMessage(ranges)
    val zooKeeperRT = nsRoot.getOrCreate(ZOOKEEPER_ROUTING_TABLE)
    zooKeeperRT.data =  rangeList.toBytes
  }

  private def storeAndPropagateRoutingTable() = {
    storeRoutingTable()
    nsRoot.waitUntilPropagated()
  }

  private def loadRoutingTable() = {
    val zkNode = nsRoot.get(ZOOKEEPER_ROUTING_TABLE)
    val rangeList = new RoutingTableMessage()
    zkNode match {
      case None => throw new RuntimeException("Can not load empty routing table")
      case Some(a) => rangeList.parse(a.data)
    }
    val partition = rangeList.partitions.map(a => new RangeType(a.startKey.map(deserializeKey(_)), a.servers))
    createRoutingTable(partition.toArray)
  }

  private def createRoutingTable(partitionHandlers: List[PartitionService]): Unit = {
    val arr = new Array[RangeType[KeyType, PartitionService]](1)
    arr(0) = new RangeType[KeyType, PartitionService](None, partitionHandlers)
    createRoutingTable(arr)
  }

  /**
   *  Just a helper class to create a table and all comparisons
   */
  private def createRoutingTable(ranges: Array[RangeType[KeyType, PartitionService]]): Unit = {
    implicit def toRichIndexedRecord[T <: IndexedRecord](i: T) = new RichIndexedRecord[T](i)
    val keySchema: Schema = getKeySchema()
    routingTable = new RangeTable[KeyType, PartitionService](ranges,
      (a: KeyType, b: KeyType) => a.compare(b),
      (a: List[PartitionService], b: List[PartitionService]) => {
        a.corresponds(b)((v1, v2) => v1.id == v2.id)
      })
  }



}
