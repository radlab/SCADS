package edu.berkeley.cs.scads.storage

import edu.berkeley.cs.scads.comm._

import scala.collection.mutable.ListBuffer

import net.lag.logging.Logger

class UpdateList {
  val updateList = new ListBuffer[(Seq[PartitionService],
                                   Array[Byte],
                                   Option[Array[Byte]])]

  def append(servers: Seq[PartitionService],
             key: Array[Byte],
             value: Option[Array[Byte]]) = {
    // worry about thread safety?
    updateList.append((servers, key, value))
  }

  def print() {
    println("len: " + updateList.length)
    updateList.foreach(x => println(x))
  }

}

