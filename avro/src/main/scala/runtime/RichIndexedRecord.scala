package edu.berkeley.cs.avro
package runtime

import java.io.ByteArrayOutputStream

import scala.collection.JavaConversions._

import org.apache.avro.io.{BinaryDecoder, BinaryEncoder, EncoderFactory, DecoderFactory, JsonDecoder, JsonEncoder}
import org.apache.avro.generic.{GenericData, GenericDatumReader, GenericDatumWriter, IndexedRecord}
import org.apache.avro.specific.SpecificRecord

/**
 * Collection of implicit conversions to scala-ify the Avro Java Library.
 */
class RichIndexedRecord[T <: IndexedRecord](val rec: T) {
  lazy val reader = new GenericDatumReader[T](rec.getSchema())

  final def toJson: String = {
    val outBuffer = new ByteArrayOutputStream
    val encoder = EncoderFactory.get().jsonEncoder(rec.getSchema(),outBuffer)
    val writer = new GenericDatumWriter[IndexedRecord](rec.getSchema())
    writer.write(rec, encoder)
    encoder.flush()
    new String(outBuffer.toByteArray)
  }

  final def toBytes: Array[Byte] = {
    val outBuffer = new ByteArrayOutputStream
    val encoder = EncoderFactory.get().binaryEncoder(outBuffer,null)
    val writer = new GenericDatumWriter[IndexedRecord](rec.getSchema())
    writer.write(rec, encoder)
    encoder.flush
    outBuffer.toByteArray
  }

  final def parse(data: String): T = {
    val decoder = DecoderFactory.get().jsonDecoder(rec.getSchema(),data)
    reader.read(rec, decoder)
  }

  final def parse(data: Array[Byte]): T = {
    val decoder = DecoderFactory.get().binaryDecoder(data, null)
    reader.read(rec, decoder)
  }

  final def toGenericRecord: GenericData.Record = {
    val genRec = new GenericData.Record(rec.getSchema())
    rec.getSchema().getFields.foreach(f => genRec.put(f.pos, rec.get(f.pos) match {
      case r: IndexedRecord => r.toGenericRecord
      case o => o
    }))
    genRec
  }

  final def toSpecificRecord[SR <: SpecificRecord](implicit m: Manifest[SR]): SR = {
    val specific = m.erasure.newInstance.asInstanceOf[SR]
    ScalaSpecificRecordHelpers.fromGenericRecord(specific, rec)
    specific
  }

  @inline final def compare(lhs: T): Int = GenericData.get.compare(rec, lhs, rec.getSchema())
  final def >(lhs: T): Boolean = compare(lhs) > 0
  final def <(lhs: T): Boolean = compare(lhs) < 0
  final def >=(lhs: T): Boolean = compare(lhs) >= 0
  final def <=(lhs: T): Boolean = compare(lhs) <= 0
  final def ==(lhs: T): Boolean = compare(lhs) == 0
}
