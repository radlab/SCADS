package edu.berkeley.cs.avro
package runtime

import java.io.File
import org.apache.avro.specific.{SpecificDatumReader, SpecificDatumWriter}
import org.apache.avro.file.{DataFileReader, DataFileWriter, CodecFactory}

object AvroInFile {
  def apply[RecordType <: ScalaSpecificRecord](file: File)(implicit schema: TypedSchema[RecordType]): DataFileReader[RecordType] with Iterator[RecordType] = {
    new DataFileReader[RecordType](file, new SpecificDatumReader[RecordType](schema)) with Iterator[RecordType]
  }
}

object AvroOutFile {
  def apply[RecordType <: ScalaSpecificRecord](file: File, codec: Option[CodecFactory] = None)(implicit schema: TypedSchema[RecordType]): DataFileWriter[RecordType] = {
    val writer = new DataFileWriter(new SpecificDatumWriter[RecordType](schema))
    codec.foreach(writer.setCodec)
    writer.create(schema, file)
    return writer
  }
}
