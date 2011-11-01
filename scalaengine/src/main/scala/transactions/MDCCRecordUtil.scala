package edu.berkeley.cs.scads.storage.transactions

import edu.berkeley.cs.scads.storage.{MDCCRecord, AvroSpecificReaderWriter}

object MDCCRecordUtil {
  private val recordReaderWriter = new AvroSpecificReaderWriter[MDCCRecord](None)

  def toBytes(rec: MDCCRecord): Array[Byte] = {
    recordReaderWriter.serialize(rec)
  }

  def toBytes(rec: Option[Array[Byte]], metadata: MDCCMetadata): Array[Byte] = {
    recordReaderWriter.serialize(MDCCRecord(rec, metadata))
  }

  def fromBytes(bytes: Array[Byte]): MDCCRecord = {
    recordReaderWriter.deserialize(bytes)
  }  
}
