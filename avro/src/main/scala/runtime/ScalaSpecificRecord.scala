package edu.berkeley.cs.avro
package runtime

import org.apache.avro.Schema
import org.apache.avro.io.{BinaryEncoder, BinaryDecoder, DecoderFactory}
import org.apache.avro.specific.{SpecificData, SpecificDatumReader, SpecificDatumWriter, SpecificRecord}
import org.apache.avro.generic.{ GenericRecord, GenericData, IndexedRecord }

import collection.JavaConversions._
import java.io._

private[runtime] object ScalaSpecificRecordHelpers {
  def fromGenericRecord[SR <: SpecificRecord](specific: SR, generic: IndexedRecord): SR = {
    specific.getSchema.getFields.foreach(f => {
      specific.put(f.pos, generic.get(f.pos) match {
        case innerGeneric: GenericData.Record =>
          // create new instance of inner 
          val innerSpecific = SpecificData.get.getClass(f.schema).newInstance.asInstanceOf[SpecificRecord]
          fromGenericRecord(innerSpecific, innerGeneric)
        case x => x
      })
    })
    specific
  }
}

trait ScalaSpecificRecord extends SpecificRecord {

  private final lazy val __decoderFactory__ = (new DecoderFactory).configureDirectDecoder(true)
  private final lazy val __writer__ = new SpecificDatumWriter[this.type](getSchema)
  private final lazy val __reader__ = new SpecificDatumReader[this.type](getSchema) {
    override def newRecord(old: AnyRef, schema: Schema) =
      if (old ne null) old // a bit of a hack (no checking for class instance equality)
                           // but for normal usages, old should always be an
                           // appropriate instance (since our records are
                           // typesafe)
      else super.newRecord(old, schema)
  }

  def toBytes: Array[Byte] = {
    val out = new ByteArrayOutputStream(128)
    toBytes(out)
    out.toByteArray
  }

  def toBytes(outputStream: OutputStream) {
    val enc = new BinaryEncoder(outputStream)
    __writer__.write(this, enc)
  }

  def parse(data: Array[Byte]): this.type = {
    val stream = new ByteArrayInputStream(data)
    parse(stream)
  }

  def parse(inputStream: InputStream): this.type = {
    val dec = __decoderFactory__.createBinaryDecoder(inputStream, null) // new decoder
    __reader__.read(this, dec)
    this
  }

  def fromGenericRecord(generic: GenericData.Record): this.type =
    ScalaSpecificRecordHelpers.fromGenericRecord(this, generic).asInstanceOf[this.type]
}

class AvroPairGenericRecord(pair: SpecificRecord, offset: Int, schema: Schema) 
  extends GenericData.Record(schema) {
  override def get(i: Int) = 
    pair.get(i + offset)
  override def put(i: Int, v: Any) =
    pair.put(i + offset, v)
  override def get(s: String) = schema.getField(s) match {
    case null => 
      throw new RuntimeException("No such field: " + s)
    case field => get(field.pos)
  }
  override def put(s: String, v: Any) = schema.getField(s) match {
    case null => 
      throw new RuntimeException("No such field: " + s)
    case field => put(field.pos, v)
  }
}
