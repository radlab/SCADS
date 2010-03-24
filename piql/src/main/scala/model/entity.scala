package edu.berkeley.cs.scads.piql

import org.apache.avro.Schema
import org.apache.avro.io.BinaryDecoder
import org.apache.avro.specific.SpecificRecordBase
import org.apache.avro.specific.SpecificRecord
import org.apache.avro.specific.SpecificDatumReader
import java.io.ByteArrayInputStream

import edu.berkeley.cs.scads.storage.{ScadsCluster, Namespace}

class PiqlDataReader(schema: Schema) extends SpecificDatumReader[Any](schema) {
	override def newRecord(old: Object, schema: Schema): Object = {
		return old
	}
}

abstract class EntityPart extends SpecificRecordBase with SpecificRecord {
	val reader = new PiqlDataReader(getSchema)

	override def parse(bytes: Array[Byte]): Unit = {
		val in = new ByteArrayInputStream(bytes)
		val dec = new BinaryDecoder(in)
		reader.read(this, dec)
	}
}

abstract class Entity(cluster: ScadsCluster) {
  val namespace: Namespace
  val key: EntityPart
  val value: EntityPart

  def load(pk: SpecificRecordBase): Unit = {
    val storedValue = namespace.get(pk)
    val keyArr = new Array[Byte](storedValue.key.remaining)
    val valueArr = new Array[Byte](storedValue.value.remaining)
    storedValue.key.get(keyArr)
    storedValue.value.get(valueArr)

    println(keyArr.toList)
    key.parse(keyArr)
    println(valueArr.toList)
    value.parse(valueArr)
  }

  def save: Unit = {
    println("Storing value: " + value.toBytes.toList + " to key: " + key.toBytes.toList)
    namespace.put(key,value)
  }
}
