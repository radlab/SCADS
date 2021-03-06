package edu.berkeley.cs.scads.storage

import java.io.ByteArrayInputStream
import java.nio._

import org.apache.avro._ 
import generic._
import io._
import specific._

trait SimpleRecordMetadataExtractor {
  def extractMetadataAndRecordFromValue(value: Array[Byte]): (Array[Byte], Array[Byte]) = {
    assert(value.length >= 16, "value array is too small to be valid")
    // be explicit for performance reasons
    val rhs_len = value.length - 16
    val lhs = new Array[Byte](16)
    val rhs = new Array[Byte](rhs_len)
    System.arraycopy(value, 0, lhs, 0, 16)
    System.arraycopy(value, 16, rhs, 0, rhs_len)
    (lhs, rhs)
  }

  def extractRecordFromValue(value: Array[Byte]): Array[Byte] = {
    assert(value.length >= 16, "value array is too small to be valid")
    val rhs_len = value.length - 16
    val rhs = new Array[Byte](rhs_len)
    System.arraycopy(value, 16, rhs, 0, rhs_len)
    rhs
  }

  def extractMetadataFromValue(value: Array[Byte]): Array[Byte] = {
    assert(value.length >= 16, "value array is too small to be valid")
    val mdarray = new Array[Byte](16)
    System.arraycopy(value, 0, mdarray, 0, 16)
    mdarray
  }

  def getRecordInputStreamFromValue(value: Array[Byte]): ByteArrayInputStream = {
    assert(value.length >= 16, "value array is too small to be valid")
    new ByteArrayInputStream(value,16,(value.length-16))
  }
}

trait SimpleRecordMetadata extends RecordMetadata
  with SimpleRecordMetadataExtractor
  with GlobalMetadata {

  val cluster: ScadsCluster

  override def compareKey(x: Array[Byte], y: Array[Byte]): Int = 
    BinaryData.compare(x, 0, y, 0, keySchema)
 
  override def hashKey(x: Array[Byte]): Int = {
    // TODO: use some more awesome hash function

    // same hash function as java String for now
    var hash = 0
    var idx = 0
    val len = x.length
    while (idx < len) {
      hash = 31 * hash + x(idx)
      idx += 1
    }
    hash
  }

  override def createMetadata(rec: Array[Byte]): Array[Byte] = {
    val buffer = ByteBuffer.allocate(rec.length + 16)
    buffer.putLong(System.currentTimeMillis)
    buffer.putLong(cluster.clientID)
    buffer.put(rec)
    buffer.array
  }

  override def compareMetadata(lhs: Array[Byte], rhs: Array[Byte]): Int = {
    var idx = 0
    while (idx < 16) {
      if (lhs(idx) == rhs(idx)) {} // common case
      else if ((lhs(idx) < rhs(idx)) ^ ((lhs(idx) < 0) != (rhs(idx) < 0))) //bitwise comparison for unsigned Bytes
        return -1
      else
        return 1
      idx += 1
    }
    return 0
  }

}
