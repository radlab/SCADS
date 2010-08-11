package com.googlecode.avro
package runtime

import java.io.File
import org.apache.avro.specific.{SpecificDatumReader, SpecificDatumWriter}
import org.apache.avro.file.{DataFileReader, DataFileWriter}

object AvroInFile {
  def apply[RecordType <: ScalaSpecificRecord](file: File)(implicit schema: TypedSchema[RecordType]): DataFileReader[RecordType] = {
    new DataFileReader[RecordType](file, new SpecificDatumReader[RecordType](schema)) 
  }
}

object AvroOutFile {
  def apply[RecordType <: ScalaSpecificRecord](file: File)(implicit schema: TypedSchema[RecordType]): DataFileWriter[RecordType] = {
    val writer = new DataFileWriter(new SpecificDatumWriter[RecordType](schema))
    writer.create(schema, file)
    return writer
  }
}
