package edu.berkeley.cs.scads.piql

import org.apache.avro.Schema
import org.apache.avro.io.BinaryDecoder
import org.apache.avro.specific.SpecificRecordBase
import org.apache.avro.specific.SpecificRecord
import org.apache.avro.specific.SpecificDatumReader

import java.io.InputStream
import java.io.ByteArrayInputStream
import java.nio.ByteBuffer

import edu.berkeley.cs.scads.storage.{ScadsCluster, Namespace}

class PiqlDataReader(schema: Schema) extends SpecificDatumReader[Any](schema) {
	override def newRecord(old: Object, schema: Schema): Object = {
		return old
	}
}

abstract class EntityPart extends SpecificRecordBase with SpecificRecord {
	val reader = new PiqlDataReader(getSchema)

  override def parse(bytes: ByteBuffer): Unit = {
    val buff = bytes.duplicate
    val in = new InputStream() {
      override def read(): Int = {
        if(buff.remaining > 0)
          buff.get & 0x00FF
        else
          -1
      }
    }
		val dec = new BinaryDecoder(in)
    reader.read(this, dec)
  }

	override def parse(bytes: Array[Byte]): Unit = {
		val in = new ByteArrayInputStream(bytes)
		val dec = new BinaryDecoder(in)
		reader.read(this, dec)
	}
}

abstract class Entity[KeyType <: SpecificRecordBase, ValueType <: SpecificRecordBase](implicit keyType: scala.reflect.Manifest[KeyType], valueType: scala.reflect.Manifest[ValueType]) {
  val namespace: String

  val key: KeyType
  val value: ValueType

  val indexes: Map[String, Schema]

  def load(pk: KeyType)(implicit cluster: ScadsCluster): Unit = {
    key.parse(pk.toBytes)
    cluster.getNamespace[KeyType, ValueType](namespace).get(pk, value)
  }

  def save(implicit cluster: ScadsCluster): Unit = {
    println("Storing value: " + value.toBytes.toList + " to key: " + key.toBytes.toList)
    cluster.getNamespace[KeyType, ValueType](namespace).put(key,value)
  }
}
