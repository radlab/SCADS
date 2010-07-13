package edu.berkeley.cs.scads.storage

import java.util.Comparator
import org.apache.avro.Schema
import java.nio.ByteBuffer

@serializable
class AvroComparator(val json: String) extends Comparator[Array[Byte]] with java.io.Serializable {
  @transient
  lazy val schema = Schema.parse(json)

  def compare(o1: Array[Byte], o2: Array[Byte]): Int = {
    org.apache.avro.io.BinaryData.compare(o1, 0, o2, 0, schema)
  }

  def compare(o1: ByteBuffer, o2: Array[Byte]): Int = {
    if (!o1.hasArray)
      throw new Exception("Can't compare without backing array")
    org.apache.avro.io.BinaryData.compare(o1.array(), o1.position, o2, 0, schema)
  }

  def compare(o1: Array[Byte], o2: ByteBuffer): Int = {
    if (!o2.hasArray)
      throw new Exception("Can't compare without backing array")
    org.apache.avro.io.BinaryData.compare(o1, 0, o2.array, o2.position, schema)
  }

  override def equals(other: Any): Boolean = other match {
    case ac: AvroComparator => json equals ac.json
    case _ => false
  }
}
