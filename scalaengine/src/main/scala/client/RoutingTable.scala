package edu.berkeley.cs.scads.storage.routing

import edu.berkeley.cs.scads.comm._
import collection.mutable.HashMap
import org.apache.avro.generic.IndexedRecord
import collection.immutable.{TreeMap, SortedMap}
import org.apache.avro.Schema
import java.util.{Comparator, Arrays}
import edu.berkeley.cs.scads.storage.Namespace
import org.apache.zookeeper.CreateMode
import com.googlecode.avro.marker.AvroRecord
import com.googlecode.avro.runtime.AvroScala._
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
  var partCtr : Long = 0 //TODO remove when storage service creates IDs

  /**
   *  Creates a new NS with the given servers
   *  The ranges has the form (startKey, servers). The first Key has to be None
   */
  override def create(ranges : List[(Option[KeyType], List[StorageService])]){
    super.create(ranges)
    var rTable: Array[RangeType[KeyType, PartitionService]] = new Array[RangeType[KeyType, PartitionService]](ranges.size)
    var startKey : Option[KeyType] = None
    var endKey : Option[KeyType] = None
    var i = ranges.size - 1
    nsRoot.createChild("partitions", "".getBytes, CreateMode.PERSISTENT)
    for(range <- ranges.reverse){
      startKey = range._1
      val handlers = createPartitions(startKey, endKey, range._2)
      rTable(i) =  new RangeType[KeyType, PartitionService](endKey, handlers)
      endKey = startKey
      i -= 1
    }
    createRoutingTable(rTable)
    storeRoutingTable()
  }

  override def load() : Unit = {
    super.load()
    loadRoutingTable()
  }


  private def createPartitions(startKey: Option[KeyType] , endKey: Option[KeyType], servers : List[StorageService] )
        : List[PartitionService] =  {
    var handlers : List[PartitionService] = Nil
    for (server <- servers){
      server !? CreatePartitionRequest(namespace, startKey.map(serializeKey(_)), endKey.map(serializeKey(_))) match {
        case CreatePartitionResponse(partitionId, partitionActor) => handlers = partitionActor :: handlers
        case _ => throw new RuntimeException("Unexpected Message")
      }
    }
    return handlers
  }


  private def storeRoutingTable() = {
   // val ranges = routingTable.ranges.map(a => KeyRange(a.maxKey, a.values))
   // val rangeList = Partition(ranges)
   // nsRoot.createChild(ZOOKEEPER_ROUTING_TABLE, rangeList.toBytes, CreateMode.PERSISTENT)
  }

  private def loadRoutingTable() = {
    //val zkNode = nsRoot.get(ZOOKEEPER_ROUTING_TABLE)
    //val rangeList = new Partition()
    //zkNode match {
    //  case None =>  throw new RuntimeException("Can not load empty routing table")
    //  case Some(a) => rangeList.parse(a.data)
    //}
    //val partition = rangeList.partitions.map(a => new RangeType(a.endKey, a.servers))
    //createRoutingTable(partition.toArray)
  }

  private def createRoutingTable(partitionHandlers: List[PartitionService]): Unit = {
    val arr = new Array[RangeType[KeyType, PartitionService]](1)
    arr(0) =  new RangeType[KeyType, PartitionService](None, partitionHandlers)
    createRoutingTable(arr)
  }

  /**
   * Just a helper class to create a table and all comparisons
   */
  private def createRoutingTable(ranges: Array[RangeType[KeyType, PartitionService]]): Unit = {
    val keySchema: Schema = getKeySchema()
    routingTable = new RangeTable[KeyType, PartitionService]( ranges,
      (a: KeyType, b: KeyType) => a.compare(b), 
      (a: List[PartitionService], b: List[PartitionService]) => {
        a.corresponds(b)((v1, v2) => v1.id == v2.id)
      })
  }

  def serversForKey(key: KeyType): List[PartitionService] = {
    routingTable.valuesForKey(key)
  }

  def serversForRange(startKey: Option[KeyType], endKey: Option[KeyType]): List[List[PartitionService]] = {
    val ranges = routingTable.valuesForRange(startKey, endKey)
    (for (range <- ranges) yield range.values).toList
  }

  //ZooKeeper functions
  def refresh(): Unit = {
    loadRoutingTable()
  }

  def expired(): Unit = {
    //We do nothing.
  }


  def mergePartitions(mergeKey: KeyType): Unit = throw new RuntimeException("Unimplemented")


  def splitPartition(splitPoint: KeyType): List[PartitionService] = throw new RuntimeException("Unimplemented")

  def replicatePartition(splitPoint: KeyType, storageHandler: StorageService): PartitionService = throw new RuntimeException("Unimplemented")

  def deleteReplica(splitPoint: KeyType, partitionHandler: PartitionService): Unit = throw new RuntimeException("Unimplemented")

  def partitions: List[Partition] = throw new RuntimeException("Unimplemented")


}

/**
 * Range Table data structure. Allows to split the range -inf to inf in arbitrary sub-ranges and assign values per sub-range.
 * The operations are thread-safe
 *
 * Constructor assigns values to the range -inf to inf. This range can than be split by split(key). Keys are compared with the given
 * comparator
 *
 * TODO RangeTable should be fully immutable (also for adding values). Makes programming the protocols easier
 */
class RangeTable[KeyType, ValueType](
        val rTable: Array[RangeType[KeyType, ValueType]],
        val keyComparator: Comparator[RangeType[KeyType, ValueType]],
        val mergeCondition: (List[ValueType], List[ValueType]) => Boolean) {
  require(rTable.length > 0)
  require(rTable.last.maxKey == None)

  /**
   * Helper constructor for creating the Comparator
   */
  def this(rTable: Array[RangeType[KeyType, ValueType]],
           keyComp: (KeyType, KeyType) => Int,
           mergeCondition: (List[ValueType], List[ValueType]) => Boolean) = {
    this (rTable,
      new Comparator[RangeType[KeyType, ValueType]]() {
        def compare(a: RangeType[KeyType, ValueType], b: RangeType[KeyType, ValueType]): Int = {
          (a.maxKey, b.maxKey) match {
            case (None, None) => 0
            case (None, _) => 1
            case (_, None) => -1
            case (Some(a), Some(b)) => keyComp(a, b)
          }
        }
      },
      mergeCondition)
  }

  /**
   * Creates a new RangeTable from a Simple List
   */
  def this(initRanges: List[(Option[KeyType], List[ValueType])],
           keyComp: (KeyType, KeyType) => Int,
           mergeCondition: (List[ValueType], List[ValueType]) => Boolean) =
    this (initRanges.map(item => new RangeType[KeyType, ValueType](item._1, item._2)).toArray, keyComp, mergeCondition)


  /**
   * Returns the idx for the corresponding range
   */
  def idxForKey(key: Option[KeyType]): Int = {
    val pKey = new RangeType[KeyType, ValueType](key, Nil)
    val bpos = Arrays.binarySearch(rTable, pKey, keyComparator)

    if(bpos < 0) //nb is in range. This is the most common case, should be placed first
     ((bpos + 1) * -1)
    else if(bpos == rTable.length)
      throw new RuntimeException("A key always has to belong to a range. Probably the given comparison function is incorrect.")
    else if (bpos < rTable.length - 1){ //the endkey is right included not left
      bpos + 1
    }else
      bpos //that is the endkey = none case
  }

  /**
   * Returns the list of values which are attached to the range for the given key
   */
  def valuesForKey(key: KeyType): List[ValueType] = valuesForKey(Option(key))

  /**
   * Returns all values attached to the range for the given key
   */
  def valuesForKey(key: Option[KeyType]): List[ValueType] = {
    rTable(idxForKey(key)).values
  }

  /**
   * Returns the left and right range from a key.
   * If the value is inside an range and not on the Nothing is returned
   * None is not allowed as a key
   */
  def leftRightValuesForKey(key: KeyType): Option[(List[ValueType],List[ValueType])] = {
    val pKey = new RangeType[KeyType, ValueType](Option(key), Nil)
    val bpos = Arrays.binarySearch(rTable, pKey, keyComparator)
    if(bpos < 0)
      return None
    Some((rTable(bpos).values), (rTable(bpos+1).values))
  }

  /**
   * Returns all ranges from startKey to endKey
   */
  def valuesForRange(startKey: Option[KeyType], endKey: Option[KeyType]): Array[RangeType[KeyType, ValueType]] = {
    (startKey, endKey) match {
      case (None, None) => return rTable
      case (None, e) => rTable.slice(0, idxForKey(e) + 1)
      case (s, None) => rTable.slice(idxForKey(s), rTable.size)
      case (s, e) => rTable.slice(idxForKey(s), idxForKey(e) + 1)
    }
  }


  /**
   * Splits the range at the key position. The left range includes the split key
   * The values are either left or right attached.
   *
   */
  def split(key: KeyType, values: List[ValueType], leftAttached: Boolean = true): RangeTable[KeyType, ValueType] = {
    val pKey = new RangeType[KeyType, ValueType](Option(key), Nil)
    var idx = Arrays.binarySearch(rTable, pKey, keyComparator)
    if (idx > 0 && idx < rTable.size)
      return null
    else
      idx = (idx + 1) * -1
    if (leftAttached) {
      split(key, values, rTable(idx).values)
    } else {
      split(key, rTable(idx).values, values)
    }
  }

  /**
   * Splits the range at the key position. The left range includes the split key
   * The values are either left or right attached.
   *
   */
  def split(key: KeyType, leftValues: List[ValueType], rightValues: List[ValueType]): RangeTable[KeyType, ValueType] = {
    val pKey = new RangeType[KeyType, ValueType](Option(key), Nil)
    var idx = Arrays.binarySearch(rTable, pKey, keyComparator)
    if (idx > 0 && idx < rTable.size)
      return null
    else
      idx = (idx + 1) * -1
    val newRTable = new Array[RangeType[KeyType, ValueType]](rTable.size + 1)
    if (idx > 0)
      System.arraycopy(rTable, 0, newRTable, 0, idx)
    if (idx + 2 < newRTable.size)
      System.arraycopy(rTable, idx + 1, newRTable, idx + 2, rTable.length - idx - 1)
    newRTable(idx) = new RangeType(Option(key), leftValues)
    newRTable(idx + 1) = new RangeType(rTable(idx).maxKey, rightValues)
    return new RangeTable[KeyType, ValueType](newRTable, keyComparator, mergeCondition)
  }



  /**
   * Merges the ranges left and right from the key. Per default the left values overwrite the right values.
   */
  def merge(key: KeyType, deleteLeft: Boolean = true): RangeTable[KeyType, ValueType] = {
    val pKey = new RangeType[KeyType, ValueType](Option(key), Nil)
    val bpos = Arrays.binarySearch(rTable, pKey, keyComparator)
    if (bpos < 0 || bpos == rTable.length)
      return null
    if(deleteLeft){
      return merge(key, rTable(bpos + 1).values)
    }else{
      return merge(key, rTable(bpos).values)
    }
  }

  /**
   * Merges the ranges left and right from the key. The new partition is assigned the given set of servers.
   */
  def merge(key: KeyType, values: List[ValueType]): RangeTable[KeyType, ValueType] = {
    val pKey = new RangeType[KeyType, ValueType](Option(key), Nil)
    val bpos = Arrays.binarySearch(rTable, pKey, keyComparator)
    if (bpos < 0 || bpos == rTable.length)
      return null
    if (!mergeCondition(rTable(bpos).values, rTable(bpos + 1).values))
      return null
    val newRTable: Array[RangeType[KeyType, ValueType]] = new Array[RangeType[KeyType, ValueType]](rTable.size - 1)
    System.arraycopy(rTable, 0, newRTable, 0, bpos)
    System.arraycopy(rTable, bpos + 1, newRTable, bpos, newRTable.length - bpos)
    newRTable(bpos) = new RangeType(newRTable(bpos).maxKey, values)
    return new RangeTable[KeyType, ValueType](newRTable, keyComparator, mergeCondition)
  }

  def ranges : List[RangeType[KeyType, ValueType]] = rTable.toList

  def addValueToRange(key: KeyType, value: ValueType): RangeTable[KeyType, ValueType] = addValueToRange(Option(key), value)

  def addValueToRange(key: Option[KeyType], value: ValueType): RangeTable[KeyType, ValueType] = {
    val idx = idxForKey(key)
    val range = rTable(idx)
    val newRTable = rTable.clone
    newRTable(idx) = range.add(value)
    return new RangeTable[KeyType, ValueType](newRTable, keyComparator, mergeCondition)
  }

  def removeValueFromRange(key: KeyType, value: ValueType): RangeTable[KeyType, ValueType] = removeValueFromRange(Option(key), value)

  def removeValueFromRange(key: Option[KeyType], value: ValueType): RangeTable[KeyType, ValueType] = {
    val idx = idxForKey(key)
    val range = rTable(idx)
    val newRTable = rTable.clone
    newRTable(idx) = range.remove(value)
    return new RangeTable[KeyType, ValueType](newRTable, keyComparator, mergeCondition)
  }

  def replaceValues(key: Option[KeyType], values: List[ValueType]) : RangeTable[KeyType, ValueType] = {
    val idx = idxForKey(key)
    val newRTable = rTable.clone
    newRTable(idx) = new RangeType(newRTable(idx).maxKey, values)
    return new RangeTable[KeyType, ValueType](newRTable, keyComparator, mergeCondition)
  }

  override def toString = {
    "RTable(" + ("" /: rTable)(_ + " " + _) + ")"
  }

}



/**
 * Represents a range inside RangeTable.
 * TODO Should be an inner class of RangeTable, but it is impossible to create a RangeType without an existing parent object
 */
class RangeType[KeyType, ValueType](val maxKey: Option[KeyType], val values: List[ValueType]) {
  def add(value: ValueType): RangeType[KeyType, ValueType] = {
    if (values.indexOf(value) >= 0)
      throw new IllegalArgumentException("Value already exists")
    new RangeType(maxKey, value :: values)
  }

  /**
   * Removes a value from the Range
   */
  def remove(value: ValueType): RangeType[KeyType, ValueType] = {
    if (values.size == 1) {
      throw new RuntimeException("It is not allowed to delete the last element in a range")
    }
    new RangeType(maxKey, values.filterNot(_ == value))
  }

  override def toString = {
    "[" + maxKey + ":(" + (values.head.toString /: values.tail)(_ + "," + _) + ")]"
  }
}
