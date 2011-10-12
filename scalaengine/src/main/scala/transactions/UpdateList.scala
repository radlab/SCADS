package edu.berkeley.cs.scads.storage
package transactions


import edu.berkeley.cs.scads.comm._

import scala.collection.mutable.ListBuffer

import net.lag.logging.Logger

sealed trait UpdateInfo
case class ValueUpdateInfo(ns : TransactionDefaultMetadata,
                           servers: Seq[PartitionService],
                           key: Array[Byte],
                           rec: Option[Array[Byte]]) extends UpdateInfo
case class LogicalUpdateInfo(ns : TransactionDefaultMetadata,
                             servers: Seq[PartitionService],
                             key: Array[Byte],
                             rec: Option[Array[Byte]]) extends UpdateInfo

// TODO: Worry about thread safety?
class UpdateList {
  private val updateList = new ListBuffer[UpdateInfo]

  def appendValueUpdateInfo(ns : TransactionDefaultMetadata,
                            servers: Seq[PartitionService],
                            key: Array[Byte],
                            rec: Option[Array[Byte]]) = {
    updateList.append(ValueUpdateInfo(ns, servers, key, rec))
  }

  def appendLogicalUpdate(ns : TransactionDefaultMetadata,
                          servers: Seq[PartitionService],
                          key: Array[Byte],
                          rec: Option[Array[Byte]]) = {
    updateList.append(LogicalUpdateInfo(ns, servers, key, rec))
  }

  def getUpdateList() = {
    updateList.readOnly
  }

  def print() {
    println("len: " + updateList.length)
    updateList.foreach(x => println(x))
  }
}
