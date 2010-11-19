package edu.berkeley.cs.scads.storage

import org.apache.avro.generic.IndexedRecord
import java.nio.ByteBuffer

trait SimpleMetaData[KeyType <: IndexedRecord, 
                     ValueType <: IndexedRecord, 
                     RecordType <: IndexedRecord,
                     RangeType] {
  this: Namespace[KeyType, ValueType, RecordType, RangeType] 
        with AvroSerializing[KeyType, ValueType, RecordType, RangeType] => 

  protected def createRecord(value : ValueType) : Array[Byte] = {
    //val time = toByte(System.currentTimeMillis)
    //val clientID = toByte(cluster.clientID)
    val serValue = serializeValue(value)

    // TODO: varlen encoding?? saves ~1-2 bytes per record
    val buffer = ByteBuffer.allocate(serValue.length + 16)
    buffer.putLong(System.currentTimeMillis)
    buffer.putLong(cluster.clientID)
    buffer.put(serValue)
    buffer.array
  }

  protected def extractRecordTypeFromRecord(key: Array[Byte], record: Option[Array[Byte]]): Option[RecordType] = record match {
    case None => None
    case Some(bytes) if bytes.length <= 16 => None
    case Some(bytes) => 
      Some(deserializeRecordType(key, bytes.slice(16, bytes.length))) // TODO: bytes.slice is probably slow, we should be more explicit with system.arraycopy
  }

  protected def extractRangeTypeFromRecord(key: Array[Byte], record: Option[Array[Byte]]): Option[RangeType] = record match { // TODO: don't copy and paste code from previous method?
    case None => None
    case Some(bytes) if bytes.length <= 16 => None
    case Some(bytes) => 
      Some(deserializeRangeType(key, bytes.slice(16, bytes.length)))
  }

  protected def getMetaData(record : Option[Array[Byte]]) : String = {

    if(record.isDefined){
      val buffer = ByteBuffer.wrap(record.get)
      "[Time:" + buffer.getLong() + ";Client:" + buffer.getLong() + "]"
    }else{
      ""
    }
  }

  protected def compareRecord(data1 : Option[Array[Byte]], data2 : Option[Array[Byte]]) : Int = {
    if(data1.isEmpty){
      if(data2.isEmpty){
        return 0
      }
      return -1
    }
    if(data2.isEmpty){
      return 1
    }
    compareRecord(data1.get, data2.get)
  }

  protected def compareRecord(data1 : Array[Byte], data2 : Array[Byte]) : Int = {
    for(i <- 0 until 16){
      if(data1(i) == data2(i)) {   //Check common case first
      }else if((data1(i) < data2(i)) ^ ((data1(i) < 0) != (data2(i) < 0)) ){ //bitwise comparison for unsigned Bytes
        return -1
      }else{
        return 1
      }
    }
    return 0
  }

}
