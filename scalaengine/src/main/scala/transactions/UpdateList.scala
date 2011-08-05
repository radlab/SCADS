package edu.berkeley.cs.scads.storage

import edu.berkeley.cs.scads.comm._

import scala.collection.mutable.ListBuffer

import net.lag.logging.Logger

// TODO: Worry about thread safety?
class UpdateList {
  val updateList = new ListBuffer[(Seq[PartitionService], RecordUpdate)]

  def appendVersionUpdate(servers: Seq[PartitionService],
                          key: Array[Byte],
                          value: Array[Byte]) = {
    updateList.append((servers, VersionUpdate(key, value)))
  }

  def appendLogicalUpdate(servers: Seq[PartitionService],
                          key: Array[Byte],
                          schema: String,
                          value: Array[Byte]) = {
    updateList.append((servers, LogicalUpdate(key, schema, value)))
  }

  def print() {
    println("len: " + updateList.length)
    updateList.foreach(x => println(x))
  }
}
