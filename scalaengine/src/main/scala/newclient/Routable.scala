package edu.berkeley.cs.scads.storage.newclient

import edu.berkeley.cs.scads._
import comm._
import util._

import java.nio._
import java.util.{ Arrays => JArrays }

import org.apache.avro._
import io._

private[storage] object DefaultKeyRoutableLike {
  val ZOOKEEPER_ROUTING_TABLE = "routingtable"
  val ZOOKEEPER_PARTITION_ID = "partitionid"
}

trait DefaultKeyRoutableLike
  extends KeyRoutable 
  with KeyPartitionable
  with Namespace
  with GlobalMetadata
  with RecordMetadata {

  import DefaultKeyRoutableLike._

  @volatile protected var routingTable: RangeTable[Array[Byte], PartitionService] = _

  onCreate {
    // TODO: assumes that create maps 1 random server to the entire key range
    createRoutingTable(createPartitions(None, None, cluster.getRandomServers(1)))
    storeRoutingTable()
  }

  onOpen {
    loadRoutingTable()
  }

  onDelete {
    val storageHandlers = routingTable.rTable.flatMap(_.values.map(_.storageService)).toSet
    storageHandlers foreach { handler =>
      handler !? DeleteNamespaceRequest(name) match {
        case DeleteNamespaceResponse() =>
          logger.info("Successfully deleted namespace %s on StorageHandler %s", name, handler)
        case InvalidNamespace(_) =>
          logger.error("Got invalid namespace error for namespace %s on StorageService %s", name, handler)
        case e =>
          logger.error("Unexpected message from DeleteNamespaceRequest: %s", e)
      }
    }
    routingTable = null
  }

  onClose {
    // TODO
  }

  override def serversForKey(key: Array[Byte]): Seq[PartitionService] = {
    routingTable.valuesForKey(convertToRoutingKey(key))
  }

  override def onRoutingTableChanged(newTable: Array[Byte]): Unit = error("onRoutingTableChanged")

  protected def convertToRoutingKey(key: Array[Byte]): Array[Byte]

  protected def routingKeyComp: (Array[Byte], Array[Byte]) => Int

  private def storeRoutingTable() = {
    assert(validateRoutingTable(), "Holy shit, we are about to store a crappy Routing Table.")
    val ranges = routingTable.ranges.map(a => KeyRange(a.startKey, a.values))
    val rangeSeq = RoutingTableMessage(ranges)
    putMetadata(ZOOKEEPER_ROUTING_TABLE, rangeSeq.toBytes)
  }

  @inline private def storeAndPropagateRoutingTable() = {
    storeRoutingTable()
    waitUntilMetadataPropagated()
  }

  /**
   * This function tests if the routing table is in a good state.
   * Thus, all partitions in the routing table must react and all boundaries must be correct
   */
  def validateRoutingTable(): Boolean = {
    var endKey: Option[Array[Byte]] = None
    for (range <- routingTable.ranges.reverse) {
      for (partition <- range.values) {
        val keys = getStartEndKey(partition)
        if (!optArrayEq(keys._1, range.startKey) || !optArrayEq(keys._2, endKey)) {
          assert(false, "The routing table is corrupted. Partition: " + partition + " with key [" + keys._1 + "," + keys._2 + "] != [" +  range.startKey + "," + endKey + "] from table:" + routingTable)
          return false
        }
      }
      endKey = range.startKey
    }
    return true
  }

  @inline private def optArrayEq(lhs: Option[Array[Byte]], rhs: Option[Array[Byte]]): Boolean = (lhs, rhs) match {
    case (Some(x), Some(y)) => JArrays.equals(x, y)
    case _ => lhs == rhs
  }

  private def getStartEndKey(partitionHandler: PartitionService): (Option[Array[Byte]], Option[Array[Byte]]) = {
    // This request could be avoided if we would store the ranges in the partitionHandler.
    // However replication is not on the critical path and so expensive anyway that it does not matter

    val keys = partitionHandler !? GetResponsibilityRequest() match {
      case GetResponsibilityResponse(s, e) => (s, e)
      case _ => throw new RuntimeException("Unexpected Message")
    }
    return keys
  }

  private def loadRoutingTable() = getMetadata(ZOOKEEPER_ROUTING_TABLE) match {
    case None => throw new RuntimeException("Can not load empty routing table")
    case Some(data) => 
      val rangeSeq = new RoutingTableMessage
      rangeSeq.parse(data)
      val partition = rangeSeq.partitions.map(a => new RangeType(a.startKey, a.servers))
      createRoutingTable(partition.toArray)
  }

  @inline private def createRoutingTable(partitionHandlers: Seq[PartitionService]): Unit = {
    val arr = new Array[RangeType[Array[Byte], PartitionService]](1)
    arr(0) = new RangeType[Array[Byte], PartitionService](None, partitionHandlers)
    createRoutingTable(arr)
  }

  private val mergeCond = (a: Seq[PartitionService], b: Seq[PartitionService]) => {
    a.corresponds(b)((v1, v2) => v1.storageService.id == v2.storageService.id)
  }

  @inline private def createRoutingTable(ranges: Array[RangeType[Array[Byte], PartitionService]]): Unit = {
    routingTable = new RangeTable[Array[Byte], PartitionService](
      ranges,
      routingKeyComp, 
      mergeCond)
  }

  private def createPartitions(startKey: Option[Array[Byte]], endKey: Option[Array[Byte]], servers: Seq[StorageService]): Seq[PartitionService] = {
    for (server <- servers) yield {
      server !? CreatePartitionRequest(name, startKey, endKey) match {
        case CreatePartitionResponse(partitionActor) => partitionActor
        case _ => throw new RuntimeException("Unexpected Message")
      }
    }
  }

  /** simply issues DeletePartitionRequests to the partition handlers, does
   * not modify the routing table. use deletePartitions to properly delete a
   * partition service */
  private def deletePartitionServices(partitions: Seq[PartitionService]): Unit = {
    for (partition <- partitions) {
      partition.storageService !? DeletePartitionRequest(partition.partitionId) match {
        case DeletePartitionResponse() => ()
        case _ => throw new RuntimeException("Unexpected Message")
      }
    }
  }

  /** Keys are input as regular keys. the method will convert them to routing
   * table keys */
  override def splitPartition(splitKeys: Seq[Array[Byte]]): Unit = {
    val routingSplitKeys = splitKeys.map(convertToRoutingKey)
    val oldPartitions = for (splitPoint <- routingSplitKeys) yield {
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
    // TODO: should execute deletions in parallel, with ftchs
    for (oldPartition <- oldPartitions)
      deletePartitionServices(oldPartition)
  }

  override def mergePartition(mergeKeys: Seq[Array[Byte]]): Unit = {
    val routingMergeKeys = mergeKeys.map(convertToRoutingKey)
    val oldPartitions = for (mergeKey <- routingMergeKeys) yield {
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

    // TODO: same as for splitPartition, could do this in parallel 
    for((leftPart, rightPart) <- oldPartitions){
      deletePartitionServices(leftPart)
      deletePartitionServices(rightPart)
    }
  }

  override def deletePartitions(partitionHandlers: Seq[PartitionService]): Unit = {
    for(partitionHandler <- partitionHandlers){
      val (startKey, endKey) = getStartEndKey(partitionHandler) //Not really needed, just an additional check
      routingTable = routingTable.removeValueFromRange(startKey, partitionHandler)
    }
    storeAndPropagateRoutingTable()
    deletePartitionServices(partitionHandlers)
  }

  override def replicatePartitions(targets: Seq[(PartitionService, StorageService)]): Seq[PartitionService] = {
    val result = for((partitionHandler, storageHandler) <- targets) yield {
      val (startKey, endKey) = getStartEndKey(partitionHandler)
      val newPartition = createPartitions(startKey, endKey, Seq(storageHandler)).head
      routingTable = routingTable.addValueToRange(startKey, newPartition)
      (newPartition, partitionHandler)
    }
    storeAndPropagateRoutingTable()
    // TODO: once again, do this in parallel
    for((newPartition, oldPartition) <- result){
      newPartition !? CopyDataRequest(oldPartition, false) match {
        case CopyDataResponse() => ()
        case _ => throw new RuntimeException("Unexpected Message")
      }
    }
    result.map(_._1)
  }

}

trait DefaultKeyRoutable extends DefaultKeyRoutableLike {
  /** No op - the key is its own routing key */
  protected def convertToRoutingKey(key: Array[Byte]) = key

  protected val routingKeyComp = compareKey _ 
}

trait DefaultHashKeyRoutable extends DefaultKeyRoutableLike {

  override protected def convertToRoutingKey(key: Array[Byte]) = {
    // here's where we apply the hash function, and return a 
    // routing key which represents the 4 bytes of the hash
    val hash = hashKey(key)

    // TODO: don't be lazy, and just write the hash directly to a byte array,
    // saving an object allocation
    val b = ByteBuffer.allocate(4)
    b.putInt(hash)
    b.array
  }

  protected val routingKeyComp = (lhs: Array[Byte], rhs: Array[Byte]) => {
    assert(lhs.length == 4 && rhs.length == 4, "bad routing keys passed to comp")

    // TODO: don't be lazy, and just pull the bytes right out of the array
    // instead of having a byte buffer do the work of converting to int
    // (saves 2 object allocations per comparison)
    val lhs_bb = ByteBuffer.wrap(lhs)
    val rhs_bb = ByteBuffer.wrap(rhs)

    // equiv to Integer.compareTo, except minus the auto-boxing
    val diff = lhs_bb.getInt - rhs_bb.getInt
    if (diff == 0) 0 
    else if (diff < 0) -1
    else 1
  }

}

trait DefaultKeyRangeRoutable extends DefaultKeyRoutable with KeyRangeRoutable {
  /** No-op b/c routing key is the same key as a range key */
  protected def convertFromRoutingKey(key: Array[Byte]): Array[Byte] = key

  override def serversForKeyRange(startKey: Option[Array[Byte]], endKey: Option[Array[Byte]]): Seq[RangeDesc] = {
    val ranges = routingTable.valuesForRange(startKey, endKey)
    val result = new Array[RangeDesc](ranges.size)
    var sKey: Option[Array[Byte]] = None
    var eKey: Option[Array[Byte]] = endKey
    // TODO: for loops are slow - replace w/ while
    for (i <- ranges.size - 1 to 0 by -1){
      if(i == 0)
        sKey = startKey
      else
        sKey = ranges(i).startKey
      result(i) = new RangeDesc(sKey.map(convertFromRoutingKey), eKey.map(convertToRoutingKey), ranges(i).values)
      eKey = sKey
    }
    result.toSeq
  }

}
