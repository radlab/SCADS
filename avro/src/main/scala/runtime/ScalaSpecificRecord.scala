package edu.berkeley.cs.avro
package runtime

import org.apache.avro.Schema
import org.apache.avro.io.{BinaryEncoder, BinaryDecoder, EncoderFactory, DecoderFactory}
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

object ScalaSpecificRecord {
  val parser = new Schema.Parser()

  //HACK: to deal with multiple compilations phases with same global object in sbt
  def parse(schema: String): Schema = new Schema.Parser().parse(schema)
}

trait ScalaSpecificRecord extends SpecificRecord {

  private final lazy val __writer__ = new SpecificDatumWriter[this.type](getSchema)
  private final lazy val __reader__ = new SpecificDatumReader[this.type](getSchema, getSchema, new SpecificData(getClass.getClassLoader))

  def toBytes: Array[Byte] = {
    val out = new ByteArrayOutputStream(128)
    toBytes(out)
    out.toByteArray
  }

  def toBytes(outputStream: OutputStream) {
    val enc = EncoderFactory.get().binaryEncoder(outputStream,null)
    __writer__.write(this, enc)
    enc.flush()
  }

  def parse(data: Array[Byte]): this.type = {
    val stream = new ByteArrayInputStream(data)
    parse(stream)
  }

  def parse(inputStream: InputStream): this.type = {
    val dec = DecoderFactory.get().directBinaryDecoder(inputStream, null)
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
