package edu.berkeley.cs.scads.storage

import org.apache.avro.generic.IndexedRecord
import java.nio.ByteBuffer

abstract trait SimpleMetaData[KeyType <: IndexedRecord, ValueType <: IndexedRecord] extends Namespace[KeyType, ValueType] {


  override protected def createRecord(value : ValueType) : Array[Byte] = {
    //val time = toByte(System.currentTimeMillis)
    //val clientID = toByte(cluster.clientID)
    val serValue = serializeValue(value)
    val buffer = ByteBuffer.allocate(serValue.length + 16)
    buffer.putLong(System.currentTimeMillis)
    buffer.putLong(cluster.clientID)
    buffer.put(serValue)
    buffer.array
  }

  override protected def extractValueFromRecord(record: Option[Array[Byte]]): Option[ValueType] ={
    if(record.isEmpty){
      return None
    }
    if(record.get.length == 16)
      return None
    Some(deserializeValue(record.get.slice(16, record.get.length)))
  }

  override protected def getMetaData(record : Option[Array[Byte]]) : String = {

    if(record.isDefined){
      val buffer = ByteBuffer.wrap(record.get)
      "[Time:" + buffer.getLong() + ";Client:" + buffer.getLong() + "]"
    }else{
      ""
    }
  }

  override protected def compareRecord(data1 : Option[Array[Byte]], data2 : Option[Array[Byte]]) : Int = {
    if(data1.isEmpty){
      if(data2.isEmpty){
        return 0
      }
      return 1
    }
    if(data2.isEmpty){
      return -1
    }
    compareRecord(data1.get, data2.get)
  }

  override protected def compareRecord(data1 : Array[Byte], data2 : Array[Byte]) : Int = {
    for(i <- 0 until 16){
      if(! (data1(i).compare(data2(i)) == 0))
        return data1(i).compare(data2(i))
    }
    return 0
  }

//
//  private def toByte(data : Long) : Array[Byte] = {
//    return new Array[Byte] (
//        ((data >> 56) & 0xff).toByte,
//        ((data >> 48) & 0xff).toByte,
//        ((data >> 40) & 0xff).toByte,
//        ((data >> 32) & 0xff).toByte,
//        ((data >> 24) & 0xff).toByte,
//        ((data >> 16) & 0xff).toByte,
//        ((data >> 8) & 0xff).toByte,
//        ((data >> 0) & 0xff).toByte
//    )
//  }
//
//  private def toLong(data : Array[Byte]) : Long = {
//    require (data != null && data.length == 8)
//    // ----------
//    return (
//            // (Below) convert to longs before shift because digits
//            //         are lost with ints beyond the 32-bit limit
//            (0xff & data(0)).toLong << 56  |
//            (0xff & data(1)).toLong << 48  |
//            (0xff & data(2)).toLong << 40  |
//            (0xff & data(3)).toLong << 32  |
//            (0xff & data(4)).toLong << 24  |
//            (0xff & data(5)).toLong << 16  |
//            (0xff & data(6)).toLong << 8   |
//            (0xff & data(7)).toLong << 0
//            )
//  }


}