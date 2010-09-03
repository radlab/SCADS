package com.googlecode.avro
package runtime

import org.apache.avro.io.{BinaryEncoder, BinaryDecoder, DecoderFactory}
import org.apache.avro.specific.{SpecificDatumReader, SpecificDatumWriter, SpecificRecord}

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

trait ScalaSpecificRecord extends SpecificRecord {
  def toBytes: Array[Byte] = {
    val out    = new ByteArrayOutputStream
    val enc    = new BinaryEncoder(out)
    val writer = new SpecificDatumWriter[ScalaSpecificRecord](getSchema)
    writer.write(this, enc)
    out.toByteArray
  }

  def parse(data: Array[Byte]): this.type = {
    val stream  = new ByteArrayInputStream(data)
    val factory = new DecoderFactory
    val dec     = factory.createBinaryDecoder(stream, null) // new decoder
    val reader  = new SpecificDatumReader[this.type](getSchema);
    reader.read(this, dec)
    this
  }
}
