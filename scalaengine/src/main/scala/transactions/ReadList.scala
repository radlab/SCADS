package edu.berkeley.cs.scads.storage
package transactions

import scala.collection.mutable.HashMap
import net.lag.logging.Logger

import mdcc.ByteArrayWrapper
import java.util.concurrent.ConcurrentHashMap

// TODO: Worry about thread safety?
class ReadList {
  val readList = new ConcurrentHashMap[ByteArrayWrapper, (MDCCRecord, Array[Byte])]

  // TODO: does the namespace have to be considered?
  def addRecord(key: Array[Byte], record: MDCCRecord, recordBytes: Array[Byte]) = {
    readList.put(new ByteArrayWrapper(key), (record, recordBytes))
  }

  def getRecord(key: Array[Byte]): Option[(MDCCRecord, Array[Byte])] = {
    readList.get(new ByteArrayWrapper(key))
  }

  def print() {
    println("len: " + readList.size)
    readList.foreach(x => println(x))
  }
}
