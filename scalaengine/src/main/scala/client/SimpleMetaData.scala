package edu.berkeley.cs.scads.storage

import org.apache.avro.generic.IndexedRecord
import java.nio.ByteBuffer

trait SimpleMetaData[KeyType <: IndexedRecord, ValueType <: IndexedRecord] {
  this: Namespace[KeyType, ValueType] with AvroSerializing[KeyType, ValueType] => 


  protected def createRecord(value : ValueType) : Array[Byte] = {
    //val time = toByte(System.currentTimeMillis)
    //val clientID = toByte(cluster.clientID)
    val serValue = serializeValue(value)
    val buffer = ByteBuffer.allocate(serValue.length + 16)
    buffer.putLong(System.currentTimeMillis)
    buffer.putLong(cluster.clientID)
    buffer.put(serValue)
    buffer.array
  }

  protected def extractValueFromRecord(record: Option[Array[Byte]]): Option[ValueType] ={
    if(record.isEmpty){
      return None
    }
    if(record.get.length == 16)
      return None
    Some(deserializeValue(record.get.slice(16, record.get.length)))
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
