package edu.berkeley.cs.scads.piql

import org.apache.avro.Schema
import org.apache.avro.io.BinaryDecoder
import org.apache.avro.specific.SpecificRecordBase
import org.apache.avro.specific.SpecificRecord
import org.apache.avro.specific.SpecificDatumReader
import java.io.ByteArrayInputStream

class PiqlDataReader(schema: Schema) extends SpecificDatumReader[Any](schema) {
	override def newRecord(old: Object, schema: Schema): Object = {
		return old
	}
}

abstract class Entity extends SpecificRecordBase with SpecificRecord {
	val reader = new PiqlDataReader(getSchema)

	override def parse(bytes: Array[Byte]): Unit = {
		val in = new ByteArrayInputStream(bytes)
		val dec = new BinaryDecoder(in)
		reader.read(this, dec)
	}
}
