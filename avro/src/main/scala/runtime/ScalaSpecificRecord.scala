package com.googlecode.avro
package runtime

import org.apache.avro.Schema
import org.apache.avro.io.{BinaryEncoder, BinaryDecoder, DecoderFactory}
import org.apache.avro.specific.{SpecificDatumReader, SpecificDatumWriter, SpecificRecord}

import java.io._

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

}
