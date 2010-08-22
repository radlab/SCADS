package com.googlecode.avro
package runtime

import java.io.ByteArrayOutputStream

import scala.collection.JavaConversions._

import org.apache.avro.io.{BinaryDecoder, BinaryEncoder, DecoderFactory, JsonDecoder, JsonEncoder}
import org.apache.avro.generic.{GenericData, GenericDatumReader, GenericDatumWriter, IndexedRecord}

/**
 * Collection of implicit conversions to scala-ify the Avro Java Library.
 */
class RichIndexedRecord[T <: IndexedRecord](val rec: T) extends JsonRecordParser[T] {
  lazy val reader = new GenericDatumReader[T](rec.getSchema())

  @inline def toJson: String = {
    val outBuffer = new ByteArrayOutputStream
    val encoder = new JsonEncoder(rec.getSchema(), outBuffer)
    val writer = new GenericDatumWriter[IndexedRecord](rec.getSchema())
    writer.write(rec, encoder)
    encoder.flush()
    new String(outBuffer.toByteArray)
  }

  @inline def toBytes: Array[Byte] = {
    val outBuffer = new ByteArrayOutputStream
    val encoder = new BinaryEncoder(outBuffer)
    val writer = new GenericDatumWriter[IndexedRecord](rec.getSchema())
    writer.write(rec, encoder)
    outBuffer.toByteArray
  }

  @inline def parse(data: String): T = {
    val decoder = new JsonDecoder(rec.getSchema, data)
    reader.read(rec, decoder)
  }

  @inline def parse(data: Array[Byte]): T = {
    val decoder = DecoderFactory.defaultFactory().createBinaryDecoder(data, null)
    reader.read(rec, decoder)
  }

  @inline def toGenericRecord: GenericData.Record = {
    val genRec = new GenericData.Record(rec.getSchema())
    rec.getSchema().getFields().foreach(f => genRec.put(f.pos, rec.get(f.pos)))
    genRec
  }

  def compare(lhs: T): Int = GenericData.get.compare(rec, lhs, rec.getSchema())
  def >(lhs: T): Boolean = compare(lhs) > 0
  def <(lhs: T): Boolean = compare(lhs) < 0
  def >=(lhs: T): Boolean = compare(lhs) >= 0
  def <=(lhs: T): Boolean = compare(lhs) <= 0
  def ==(lhs: T): Boolean = compare(lhs) == 0
}
