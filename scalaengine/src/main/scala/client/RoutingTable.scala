package edu.berkeley.cs.scads.storage

import edu.berkeley.cs.scads.comm._
import collection.mutable.HashMap
import org.apache.avro.generic.IndexedRecord
import collection.immutable.{TreeMap, SortedMap}
import org.apache.avro.Schema
import java.util.Arrays
/* TODO: Stack RepartitioningProtocol on Routing Table to build working implementation */
trait RepartitioningProtocol[KeyType <: IndexedRecord] extends RoutingTable[KeyType] {
  override def splitPartition(splitPoint: KeyType): List[PartitionService] = throw new RuntimeException("Unimplemented")
  override def mergePartitions(mergeKey: KeyType): Unit = throw new RuntimeException("Unimplemented")
  override def replicatePartition(splitPoint: KeyType, storageHandler: StorageService): PartitionService = throw new RuntimeException("Unimplemented")
  override def deleteReplica(splitPoint: KeyType, partitionHandler: PartitionService): Unit = throw new RuntimeException("Unimplemented")
}

trait RoutingTable[KeyType <: IndexedRecord]  {
  /* Handle to the root node of the scads namespace */
  protected val nsNode: ZooKeeperProxy#ZooKeeperNode




  case class KeyRange(startKey: Option[KeyType], endKey: Option[KeyType])
  case class Partition(range: KeyRange, servers: List[PartitionService])

  def createRoutingTable(storageHandlers: List[StorageService]): Unit = {

       
  }

  def serversForKey(key: KeyType): List[PartitionService] = throw new RuntimeException("Unimplemented")
  def serversForRange(startKey: Option[KeyType], endKey: Option[KeyType]): List[(KeyRange, List[RemoteActor])] = throw new RuntimeException("Unimplemented")

  /* Zookeeper functions */
  def refresh(): Unit = throw new RuntimeException("Unimplemented")
  def expired(): Unit = throw new RuntimeException("Unimplemented")


  /* Returns the newly created PartitionServices from the split */
  def splitPartition(splitPoint: KeyType): List[PartitionService] = throw new RuntimeException("Unimplemented")
  def mergePartitions(mergeKey: KeyType): Unit = throw new RuntimeException("Unimplemented")
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
 */
class RTable [KeyType, ValueType](values : List[ValueType], val comparator : (KeyType, KeyType) => Int){

  case class Range(val maxKey: Option[KeyType], val values : List[ValueType]) extends Comparable[Range] {
    def compareTo(p:Range):Int = {
      (maxKey, p.maxKey) match {
        case (None, None) => 0
        case (None, _) => 1
        case (_, None) => -1
        case (Some(a), Some(b)) => comparator(a,b)
      }
    }

    def add(value : ValueType) : Range = {
      if(values.indexOf(value) < 0 )
        new Range(maxKey, value :: values)
      else
        throw new IllegalArgumentException("Value already exists")
    }

    def remove(value : ValueType) : Range = {
      if(values.size == 1){
        throw new RuntimeException("It is not allowed to delete the last element in a range")
      }
      new Range(maxKey, values.filterNot(_.equals(value)))
    }

    override def toString = {
      "[" + maxKey + ":(" + (values.head.toString /: values.tail) (_ + "," + _) + ")]"
    }
  }

  var rTable : Array[Range] = Array[Range](new Range(None, values))

  /**
   * Returns the idx for the corresponding range
   */
  def idxForKey(key: KeyType) : Int = {
    val pKey = new Range(Option(key),Nil)
    val bpos = Arrays.binarySearch(rTable,pKey,null)
    if (bpos < 0)
      ((bpos+1) * -1)
    else
      bpos
  }

  /**
   * Returns all values attached to the range for the given key
   */
  def valuesForKey(key: KeyType) : List[ValueType] = {
    rTable(idxForKey(key)).values
  }


  /**
   * Splits the range at the key position. The left range includes the split key
   * The values are either left or right attached.
   * A split is thread-safe for lookups (but concurrent value addings or merges might be lost)
   *
   */
  def split(key: KeyType, values : List[ValueType], leftAttached :Boolean = true) = {
    val pKey = new Range(Option(key),Nil)
    var idx = Arrays.binarySearch(rTable,pKey,null)
    if (idx > 0 && idx < rTable.size)
      throw new IllegalArgumentException("Split key already splits an existing range")
    else
       idx = (idx+1) * -1
    val newRTable = new Array[Range](rTable.size + 1)
    if(idx > 0)
      System.arraycopy(rTable,0,newRTable,0,idx)
    if(leftAttached){
      newRTable(idx) = new Range(Option(key), values)
      System.arraycopy(rTable,idx,newRTable,idx+1, rTable.length - idx)
    }else{
      newRTable(idx) = new Range(Option(key), rTable(idx).values)
      newRTable(idx+1) = new Range(rTable(idx).maxKey, values)
      if(idx + 2 < newRTable.size)
        System.arraycopy(rTable,idx+1,newRTable,idx+2, rTable.length - idx - 1)
    }
    rTable = newRTable
  }

  /**
   * Merges the ranges left and right from the key. Per default the left values overwrite the right values.
   * A merge is thread-safe for lookups (but concurrent value addings or splits might be lost)
   */
  def merge(key: KeyType, deleteLeft :Boolean = true) : Unit = {
    val pKey = new Range(Option(key),Nil)
    val bpos = Arrays.binarySearch(rTable,pKey,null)
    if(bpos< 0 || bpos == rTable.length)
      throw new IllegalArgumentException("Key has to be a maxKey (i.e., split key) of a range")
    val newRTable : Array[Range] = new Array[Range](rTable.size - 1)
    System.arraycopy(rTable,0,newRTable,0, bpos)
    System.arraycopy(rTable,bpos+1,newRTable,bpos, newRTable.length - bpos)
    if(!deleteLeft){
      newRTable(bpos) = new Range(newRTable(bpos).maxKey, rTable(bpos).values)
    }
    rTable = newRTable
    return true
  }

  def ranges : List[Range] = rTable.toList

  def add(key : KeyType, value : ValueType) = {
    val idx = idxForKey(key)
    val range = rTable(idx)
    rTable(idx) = range.add(value)
  }

  def remove(key : KeyType, value : ValueType) = {
    val idx = idxForKey(key)
    val range = rTable(idx)
    rTable(idx) = range.remove(value)
  }


  override def toString = {
    "RTable(" + ("" /: rTable) (_ + " " + _) + ")"

  }



}