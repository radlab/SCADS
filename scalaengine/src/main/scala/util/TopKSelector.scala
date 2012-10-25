package edu.berkeley.cs.scads
package storage

import org.apache.avro.generic._
import org.apache.avro.io.DecoderFactory
import org.apache.avro.Schema

import scala.collection.JavaConversions._

import java.util.Comparator
import java.util.concurrent.PriorityBlockingQueue
import org.apache.avro.util.Utf8

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
      poll
      return true
    }
    return false
  }

  def drainToList: List[A] = {
    var out: List[A] = Nil
    while (!isEmpty) {
      out ::= poll
    }
    out
  }
}

// BinaryFieldComparator compares records using fields named by string.
// TODO there's probably a binary comparator somewhere
class BinaryFieldComparator(val fields: Seq[String],
                      val valueSchema: Schema,
                      val ascending: Boolean = false)
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
  val order = if (ascending) -1 else 1

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

class Pair(var key: IndexedRecord, var value: IndexedRecord)

class FieldOrdering(fields: Seq[String], ascending: Boolean, keySchema: Schema, valueSchema: Schema) extends Comparator[Pair] {
  type PairField = Either[Int, Int]
  type KeyField = Left[Int, Int]
  type ValueField = Right[Int,Int]

  val fieldPositions: Seq[PairField] = fields.map(f =>
    keySchema.getFields.find(_.name == f).map(x =>new KeyField(x.pos)).getOrElse(
    valueSchema.getFields.find(_.name == f).map(x => new ValueField(x.pos)).getOrElse(sys.error("Unknown Field " + f))
    )
  )

  def compareValue(x: Any, y: Any): Int = {
    val comp: Int = (x, y) match {
      case (x: Int, y: Int) => (x - y)
      case (x: Utf8, y: Utf8) => x.toString.compare(y.toString)
    }
    if(ascending) -1 * comp else  comp
  }

  def compare(x: Pair, y: Pair): Int = {
    fieldPositions.foreach {
      case Left(p) =>
        val c = compareValue(x.key.get(p), y.key.get(p))
        if (c != 0) return c
      case Right(p) =>
        val c = compareValue(x.value.get(p), y.value.get(p))
        if(c != 0) return c
    }
    return 0
  }
}
