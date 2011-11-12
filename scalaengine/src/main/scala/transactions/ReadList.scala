package edu.berkeley.cs.scads.storage
package transactions

import scala.collection.mutable.HashMap
import net.lag.logging.Logger

// TODO: Worry about thread safety?
class ReadList {
  val readList = new HashMap[List[Byte], MDCCRecord]

  // TODO: does the namespace have to be considered?
  def addRecord(key: Array[Byte], record: MDCCRecord) = {
    readList.put(key.toList, record)
  }

  def getRecord(key: Array[Byte]): Option[MDCCRecord] = {
    readList.get(key.toList)
  }

  def print() {
    println("len: " + readList.size)
    readList.foreach(x => println(x))
  }
}
