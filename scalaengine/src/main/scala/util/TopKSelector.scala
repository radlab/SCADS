package edu.berkeley.cs.scads
package storage

import org.apache.avro.generic._
import org.apache.avro.io.DecoderFactory
import org.apache.avro.Schema

import scala.collection.JavaConversions._

import java.util.Comparator
import java.util.concurrent.PriorityBlockingQueue

// TruncatingQueue retains only the top k items offered.
class TruncatingQueue[A](k: Int, cmp: Comparator[A])
  extends PriorityBlockingQueue(k, cmp) {

  override def offer(item: A): Boolean = {
    if (size < k) {
      super.offer(item)
      return true
    }
    if (cmp.compare(item, peek) > 0) {
      super.offer(item)
      synchronized {
        if (size > k) {
          poll
        }
      }
      return true
    }
    return false
  }
}

// FieldComparator compares records using fields named by string.
// TODO there's probably a binary comparator somewhere
class FieldComparator(val fields: Seq[String],
                      val valueSchema: Schema,
                      val ascending: Boolean = true)
  extends Comparator[Record]
  with SimpleRecordMetadataExtractor {

  val fieldPositions = fields.map(name => {
    val field = valueSchema.getField(name)
    if (field == null)
      throw new RuntimeException("Unknown field: " + name)
    field.pos
  })
  val reader = new GenericDatumReader[IndexedRecord](valueSchema)
  val decoder = DecoderFactory.get
  val order = if (ascending) 1 else -1

  def compare(left: Record, right: Record): Int = {
    val leftDecoded = new GenericData.Record(valueSchema)
    val rightDecoded = new GenericData.Record(valueSchema)
    val leftBytes = extractRecordFromValue(left.value.get)
    val rightBytes = extractRecordFromValue(right.value.get)
    reader.read(leftDecoded, decoder.binaryDecoder(leftBytes, null))
    reader.read(rightDecoded, decoder.binaryDecoder(rightBytes, null))
    for (pos <- fieldPositions) {
      if (leftDecoded.get(pos).asInstanceOf[Int] < rightDecoded.get(pos).asInstanceOf[Int]) {
        return -order
      } else if (leftDecoded.get(pos).asInstanceOf[Int] > rightDecoded.get(pos).asInstanceOf[Int]) {
        return order
      }
    }
    return 0
  }
}
