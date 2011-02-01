package edu.berkeley.cs
package twitterspam

import org.apache.commons.httpclient._
import org.apache.commons.httpclient.methods._

import org.apache.avro.specific.{SpecificDatumReader, SpecificDatumWriter}
import org.apache.avro.file.DataFileStream

import avro.runtime._
import avro.marker._

//TODO: Move into avro package
object AvroHttpFile {
  def apply[RecordType <: ScalaSpecificRecord](url: String)(implicit schema: TypedSchema[RecordType]) = {
    val httpClient = new HttpClient
    val getMethod = new GetMethod(url)
    httpClient.executeMethod(getMethod)
    
    new DataFileStream(getMethod.getResponseBodyAsStream, new SpecificDatumReader[RecordType](schema)) with Iterator[RecordType]
  }
}
