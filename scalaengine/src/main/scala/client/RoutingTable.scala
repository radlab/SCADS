package edu.berkeley.cs.scads.storage.routing

import edu.berkeley.cs.scads.comm._
import collection.mutable.HashMap
import org.apache.avro.generic.IndexedRecord
import collection.immutable.{TreeMap, SortedMap}
import org.apache.avro.Schema
import java.util.{Comparator, Arrays}
import edu.berkeley.cs.scads.storage.Namespace
import org.apache.zookeeper.CreateMode
/* TODO: Stack RepartitioningProtocol on Routing Table to build working implementation */
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

  case class KeyRange(startKey: Option[KeyType], endKey: Option[KeyType])
  case class Partition(range: KeyRange, servers: List[PartitionService])
  var routingTable: RangeTable[Array[Byte], PartitionService] = null

  /**
   * Initializes the RoutingTable.
   */
  override def init(){
    super.init()
    if(isNewNamespace){
      nsRoot.createChild("partitions", "".getBytes, CreateMode.PERSISTENT)
      val servers = cluster.getRandomServers(defaultReplicationFactor)
      var handlers : List[PartitionService] = Nil
      for (server <- servers){
        server !? CreatePartitionRequest(namespace, None, None) match {
          case CreatePartitionResponse(partitionId, partitionActor) => handlers = partitionActor :: handlers
          case _ => throw new RuntimeException("Unexpected Message")
        }
      }
      createRoutingTable(handlers)      
    }else{
      throw new RuntimeException("Loading a partition is not yet implemented")
    }
  }


  /**
   * Just a helper class to create a table and all comparisons
   */
  private def createRoutingTable(partitionHandlers: List[PartitionService]): Unit = {
    val keySchema: Schema = getKeySchema()
    routingTable = new RangeTable[Array[Byte], PartitionService](List((None, partitionHandlers)),
      (a: Array[Byte], b: Array[Byte]) => org.apache.avro.io.BinaryData.compare(a, 0, b, 0, keySchema),
      (a: List[PartitionService], b: List[PartitionService]) => a.corresponds(b)
                ((v1, v2) => (v1.host.compareTo(v2.host) == 0) && (v1.port == v2.port)))
  }

  def serversForKey(key: KeyType): List[PartitionService] = {
    routingTable.valuesForKey(serializeKey(key))
  }

  def serversForRange(startKey: Option[KeyType], endKey: Option[KeyType]): List[List[PartitionService]] = {
    val ranges = routingTable.valuesForRange(startKey.map(serializeKey(_)), endKey.map(serializeKey(_)))
    (for (range <- ranges) yield range.values).toList
  }

  /* Zookeeper functions */
  def refresh(): Unit = throw new RuntimeException("Unimplemented")

  def expired(): Unit = throw new RuntimeException("Unimplemented")


  def mergePartitions(mergeKey: KeyType): Unit = throw new RuntimeException("Unimplemented")

   /* Returns the newly created PartitionServices from the split */
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
    if(bpos == rTable.length)
      throw new RuntimeException("A key always has to belong to a range. Probably the given comparison function is incorrect.")
    if (bpos < 0)
      ((bpos + 1) * -1)
    else
      bpos
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
   * A split is thread-safe for lookups (but concurrent value addings or merges might be lost)
   *
   */
  def split(key: KeyType, values: List[ValueType], leftAttached: Boolean = true): RangeTable[KeyType, ValueType] = {
    val pKey = new RangeType[KeyType, ValueType](Option(key), Nil)
    var idx = Arrays.binarySearch(rTable, pKey, keyComparator)
    if (idx > 0 && idx < rTable.size)
      throw new IllegalArgumentException("Split key already splits an existing range")
    else
      idx = (idx + 1) * -1
    val newRTable = new Array[RangeType[KeyType, ValueType]](rTable.size + 1)
    if (idx > 0)
      System.arraycopy(rTable, 0, newRTable, 0, idx)
    if (leftAttached) {
      newRTable(idx) = new RangeType(Option(key), values)
      System.arraycopy(rTable, idx, newRTable, idx + 1, rTable.length - idx)
    } else {
      newRTable(idx) = new RangeType(Option(key), rTable(idx).values)
      newRTable(idx + 1) = new RangeType(rTable(idx).maxKey, values)
      if (idx + 2 < newRTable.size)
        System.arraycopy(rTable, idx + 1, newRTable, idx + 2, rTable.length - idx - 1)
    }
    return new RangeTable[KeyType, ValueType](newRTable, keyComparator, mergeCondition)
  }

  /**
   * Merges the ranges left and right from the key. Per default the left values overwrite the right values.
   * A merge is thread-safe for lookups (but concurrent value addings or splits might be lost)
   */
  def merge(key: KeyType, deleteLeft: Boolean = true): RangeTable[KeyType, ValueType] = {
    val pKey = new RangeType[KeyType, ValueType](Option(key), Nil)
    val bpos = Arrays.binarySearch(rTable, pKey, keyComparator)
    if (bpos < 0 || bpos == rTable.length)
      throw new IllegalArgumentException("Key has to be a maxKey (i.e., split key) of a range")
    if (!mergeCondition(rTable(bpos).values, rTable(bpos + 1).values))
      return null
    val newRTable: Array[RangeType[KeyType, ValueType]] = new Array[RangeType[KeyType, ValueType]](rTable.size - 1)
    System.arraycopy(rTable, 0, newRTable, 0, bpos)
    System.arraycopy(rTable, bpos + 1, newRTable, bpos, newRTable.length - bpos)
    if (!deleteLeft) {
      newRTable(bpos) = new RangeType(newRTable(bpos).maxKey, rTable(bpos).values)
    }
    return new RangeTable[KeyType, ValueType](newRTable, keyComparator, mergeCondition)
  }

  def ranges: List[(Option[KeyType], List[ValueType])] = rTable.map(item => (item.maxKey, item.values)).toList

  def addValueToRange(key: KeyType, value: ValueType): Unit = addValueToRange(Option(key), value)

  def addValueToRange(key: Option[KeyType], value: ValueType): Unit = {
    val idx = idxForKey(key)
    val range = rTable(idx)
    rTable(idx) = range.add(value)
  }

  def removeValueFromRange(key: KeyType, value: ValueType): Unit = removeValueFromRange(Option(key), value)

  def removeValueFromRange(key: Option[KeyType], value: ValueType): Unit = {
    val idx = idxForKey(key)
    val range = rTable(idx)
    rTable(idx) = range.remove(value)
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
