package edu.berkeley.cs.scads.storage

import java.util.Comparator
import org.apache.avro.Schema
import java.nio.ByteBuffer

trait AvroComparator extends Comparator[Array[Byte]]  {
  val keySchema: Schema

  def compare(o1: Array[Byte], o2: Array[Byte]): Int = {
    org.apache.avro.io.BinaryData.compare(o1, 0, o2, 0, keySchema)
  }

  def compare(o1: ByteBuffer, o2: Array[Byte]): Int = {
    if (!o1.hasArray)
      throw new Exception("Can't compare without backing array")
    org.apache.avro.io.BinaryData.compare(o1.array(), o1.position, o2, 0, keySchema)
  }

  def compare(o1: Array[Byte], o2: ByteBuffer): Int = {
    if (!o2.hasArray)
      throw new Exception("Can't compare without backing array")
    org.apache.avro.io.BinaryData.compare(o1, 0, o2.array, o2.position, keySchema)
  }

  override def equals(other: Any): Boolean = other match {
    case ac: AvroComparator => keySchema equals ac.keySchema
    case _ => false
  }
}

@serializable
class AvroBdbComparator(val json: String) extends AvroComparator with java.io.Serializable {
  @transient
  lazy val keySchema = Schema.parse(json)
}
