package edu.berkeley.cs.scads.storage
package transactions


import edu.berkeley.cs.scads.comm._
import edu.berkeley.cs.scads.storage.transactions.conflict.ArrayLT

import scala.collection.mutable.ListBuffer

import net.lag.logging.Logger

sealed trait UpdateInfo {
  var key: Array[Byte]
}
case class ValueUpdateInfo(var ns : TransactionI,
                           var servers: Seq[PartitionService],
                           var key: Array[Byte],
                           var rec: Option[Array[Byte]]) extends UpdateInfo
case class LogicalUpdateInfo(var ns : TransactionI,
                             var servers: Seq[PartitionService],
                             var key: Array[Byte],
                             var rec: Option[Array[Byte]]) extends UpdateInfo

// TODO: Worry about thread safety?
class UpdateList {
  private val updateList = new ListBuffer[UpdateInfo]

  def appendValueUpdateInfo(ns : TransactionI,
                            servers: Seq[PartitionService],
                            key: Array[Byte],
                            rec: Option[Array[Byte]]) = {
    updateList.append(ValueUpdateInfo(ns, servers, key, rec))
  }

  def appendLogicalUpdate(ns : TransactionI,
                          servers: Seq[PartitionService],
                          key: Array[Byte],
                          rec: Option[Array[Byte]]) = {
    updateList.append(LogicalUpdateInfo(ns, servers, key, rec))
  }

  def getUpdateList() = {
    updateList.sortWith((a, b) => ArrayLT.arrayLT(a.key, b.key)).readOnly
  }

  def size() = {
    updateList.size
  }

  def print() {
    println("len: " + updateList.length)
    updateList.foreach(x => println(x))
  }
}
