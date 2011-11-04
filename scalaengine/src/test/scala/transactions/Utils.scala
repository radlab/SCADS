package edu.berkeley.cs.scads.test.transactions

import org.apache.avro._
import specific._
import edu.berkeley.cs.avro.marker._
import edu.berkeley.cs.avro.runtime._

import edu.berkeley.cs.scads.comm._
import edu.berkeley.cs.scads.storage._
import edu.berkeley.cs.scads.storage.transactions._
import edu.berkeley.cs.scads.storage.transactions.conflict._

class ValueBuilder[T <: SpecificRecord](implicit m: Manifest[T]) {
  private val recBuilder = new KeyBuilder[T]
  def toBytes(metadata: MDCCMetadata, rec: T): Array[Byte] = {
    MDCCRecordUtil.toBytes(MDCCRecord(recBuilder.toBytes(rec), metadata))
  }

  def fromBytes(b: Array[Byte]): (MDCCMetadata, Option[T]) = {
    val mdccRec = MDCCRecordUtil.fromBytes(b)
    (mdccRec.metadata, mdccRec.value.map(recBuilder.fromBytes _))
  }
}

class KeyBuilder[T <: SpecificRecord](implicit m: Manifest[T]) {
  private val s = m.erasure.asInstanceOf[Class[T]].newInstance.getSchema
  private val recordReaderWriter = new AvroSpecificReaderWriter[T](Some(s))

  def toBytes(r: T): Array[Byte] = {
    recordReaderWriter.serialize(r)
  }

  def fromBytes(b: Array[Byte]): T = {
    recordReaderWriter.deserialize(b)
  }
}
