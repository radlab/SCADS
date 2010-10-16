package edu.berkeley.cs.scads.util

import java.util.{Comparator, Arrays}
import net.lag.logging.Logger

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
        val mergeCondition: (Seq[ValueType], Seq[ValueType]) => Boolean) {
  require(rTable.length > 0 , "At least the startKey (None) has to exist at any time")
  require(rTable.head.startKey == None, "First key has to be None, but was " + rTable.head.startKey )

  protected lazy val logger = Logger()

  /**
   *  Helper constructor for creating the Comparator
   */
  def this(rTable: Array[RangeType[KeyType, ValueType]],
           keyComp: (KeyType, KeyType) => Int,
           mergeCondition: (Seq[ValueType], Seq[ValueType]) => Boolean) = {
    this (rTable,
      new Comparator[RangeType[KeyType, ValueType]]() {
        def compare(a: RangeType[KeyType, ValueType], b: RangeType[KeyType, ValueType]): Int = {
          (a.startKey, b.startKey) match {
            case (None, None) => 0
            case (None, _) => -1
            case (_, None) => 1
            case (Some(a), Some(b)) => keyComp(a, b)
          }
        }
      },
      mergeCondition)
  }

  /**
   * Creates a new RangeTable from a Simple Seq
   */
  def this(initRanges: Seq[(Option[KeyType], Seq[ValueType])],
           keyComp: (KeyType, KeyType) => Int,
           mergeCondition: (Seq[ValueType], Seq[ValueType]) => Boolean) =
    this (initRanges.map(item => new RangeType[KeyType, ValueType](item._1, item._2)).toArray, keyComp, mergeCondition)

  /**
   * Helper case class to return all ranges (left, center, right) for a given key
   */
  case class RangeBound(val left: RangeType[KeyType, ValueType], val center: RangeType[KeyType, ValueType], val right: RangeType[KeyType, ValueType])


  private def binarySearch(key: KeyType): Int = {
    binarySearch(Some(key))
  }


  private def binarySearch(key: Option[KeyType]): Int = {
    val pKey = new RangeType[KeyType, ValueType](key, Nil)
    Arrays.binarySearch(rTable, pKey, keyComparator)
  }

  /**
   * If the key is inside the range binarysearch returns a neg. value. This calc corrects it.
   */
  private def calcMidIdx(idx: Int): Int = (idx * -1) - 2

  /**
   * Returns the idx for the corresponding range
   */
  def idxForKey(key: Option[KeyType]): Int = {
    val bpos = binarySearch(key)

    if (bpos < 0) //nb is in range. This is the most common case, should be placed first
      calcMidIdx(bpos)
    else if (bpos < rTable.length)
      bpos
    else
      throw new RuntimeException("A key always has to belong to a range. Probably the given comparison function is incorrect.")
  }

  /**
   * Returns the list of values which are attached to the range for the given key
   */
  def valuesForKey(key: KeyType): Seq[ValueType] = valuesForKey(Option(key))

  /**
   * Returns all values attached to the range for the given key
   */
  def valuesForKey(key: Option[KeyType]): Seq[ValueType] = {
    rTable(idxForKey(key)).values
  }


  /**
   * Returns the surrounding ranges for a key
   * If the value is in the most left range starting with None, the left and center range will be the same
   * Warning: Right ranges can be just a placeholder for None with an empty set of servers
   *
   */
  def lowerUpperBound(key : KeyType) : RangeBound = lowerUpperBound(Some(key))
  def lowerUpperBound(startKey: Option[KeyType]): RangeBound = {
    val idx = idxForKey(startKey)
    val lowerBound = if (idx == 0) rTable(0) else rTable(idx - 1)
    val upperBound = if (idx + 1 == rTable.length) new RangeType[KeyType, ValueType](None, Nil) else rTable(idx + 1)
    RangeBound(lowerBound, rTable(idx), upperBound)
  }

  def isSplitKey(key: KeyType): Boolean = {
    val bpos = binarySearch(key)
    if (bpos == rTable.length)
      false
    else if (bpos > 0)
      true
    else
      false
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
   * If the splitKey already exists, null is returned.
   *
   */
  def split(key: KeyType, values: Seq[ValueType], leftAttached: Boolean = true): RangeTable[KeyType, ValueType] = {
    var idx = binarySearch(key)
    if (idx > 0 && idx < rTable.size)
      return null
    else
      idx = calcMidIdx(idx)
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
  def split(key: KeyType, leftValues: Seq[ValueType], rightValues: Seq[ValueType]): RangeTable[KeyType, ValueType] = {
    var idx = binarySearch(key)
    if (idx > 0 && idx < rTable.size)
      return null
    else
      idx = calcMidIdx(idx)
    val newRTable = new Array[RangeType[KeyType, ValueType]](rTable.size + 1)
    System.arraycopy(rTable, 0, newRTable, 0, idx)
    if (idx + 2 < newRTable.size)
      System.arraycopy(rTable, idx + 1, newRTable, idx + 2, rTable.length - idx - 1)
    newRTable(idx) = new RangeType(rTable(idx).startKey, leftValues)
    newRTable(idx + 1) = new RangeType(Option(key), rightValues)
    return new RangeTable[KeyType, ValueType](newRTable, keyComparator, mergeCondition)
  }


  /**
   * Checks if the given range is an existing range
   */
  def checkRange(startKey: Option[KeyType], endKey: Option[KeyType]): Boolean = {
    var idx = binarySearch(startKey)
    if (idx < 0) {
      false
    } else if (rTable.length == idx) {
      startKey == rTable(idx) && endKey.isEmpty
    } else {
      startKey == rTable(idx) && endKey == rTable(idx + 1)
    }
  }


  /**
   *  Merges the ranges left and right from the key. Per default the left values overwrite the right values.
   */
  def merge(key: KeyType, deleteLeft: Boolean = true): RangeTable[KeyType, ValueType] = {
    val bpos = binarySearch(key)
    if (bpos < 0 || bpos == rTable.length)
      return null
    if (deleteLeft) {
      return merge(key, rTable(bpos).values)
    } else {
      return merge(key, rTable(bpos - 1).values)
    }
  }

  def checkMergeCondition(key: KeyType): Boolean = {
    val bpos = binarySearch(key)
    checkMergeCondition(bpos)
  }

  private def checkMergeCondition(bpos: Int): Boolean = {
    if (bpos < 0 || bpos == rTable.length){
      logger.info("[%s] Merge Condition failed - key is not a split key".format(this))
      return false
    }else if (!mergeCondition(rTable(bpos - 1).values, rTable(bpos).values)){
      logger.info("[%s] Merge Condition failed - sets are not equal. $s != $s", this, rTable(bpos - 1), rTable(bpos) )   
      return false
    }else
      return true
  }


  /**
   * Merges the ranges left and right from the key. The new partition is assigned the given set of servers.
   */
  def merge(key: KeyType, values: Seq[ValueType]): RangeTable[KeyType, ValueType] = {
    val bpos = binarySearch(key)
    if (!checkMergeCondition(bpos))
      return null
    val newRTable: Array[RangeType[KeyType, ValueType]] = new Array[RangeType[KeyType, ValueType]](rTable.size - 1)
    System.arraycopy(rTable, 0, newRTable, 0, bpos)
    if (bpos != newRTable.length)
      System.arraycopy(rTable, bpos + 1, newRTable, bpos, newRTable.length - bpos)
    newRTable(bpos - 1) = new RangeType(newRTable(bpos - 1).startKey, values)
    return new RangeTable[KeyType, ValueType](newRTable, keyComparator, mergeCondition)
  }

  def ranges: Seq[RangeType[KeyType, ValueType]] = rTable.toSeq

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

  def replaceValues(key: Option[KeyType], values: Seq[ValueType]): RangeTable[KeyType, ValueType] = {
    val idx = idxForKey(key)
    val newRTable = rTable.clone
    newRTable(idx) = new RangeType(newRTable(idx).startKey, values)
    return new RangeTable[KeyType, ValueType](newRTable, keyComparator, mergeCondition)
  }

  override def toString = {
    "RTable(" + ("" /: rTable)(_ + " " + _) + ")"
  }

}


/**
 * Represents a range inside RangeTable. StartKey is included in the range
 * TODO Should be an inner class of RangeTable, but it is impossible to create a RangeType without an existing parent object
 */
case class RangeType[KeyType, ValueType](val startKey: Option[KeyType], val values: Seq[ValueType]) {
  def add(value: ValueType): RangeType[KeyType, ValueType] = {
    if (values.indexOf(value) >= 0)
      throw new IllegalArgumentException("Value already exists")
    new RangeType(startKey, value +: values)
  }

  /**
   * Removes a value from the Range
   */
  def remove(value: ValueType): RangeType[KeyType, ValueType] = {
    if (values.size == 1) {
      throw new RuntimeException("It is not allowed to delete the last element in a range")
    }
    new RangeType(startKey, values.filterNot(_ == value))
  }

  override def toString = {
    if (values.isEmpty)
      "[" + startKey + ":()]"
    else
      "[" + startKey + ":(" + (values.head.toString /: values.tail)(_ + "," + _) + ")]"
  }
}
