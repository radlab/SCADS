package edu.berkeley.cs
package scads
package perf

import comm._

import avro.runtime._
import avro.marker._

import net.lag.logging.Logger
import org.apache.avro.generic.IndexedRecord

abstract trait AvroClient extends IndexedRecord {
  val logger = Logger()

  def run(clusterRoot: ZooKeeperProxy#ZooKeeperNode)
}

object AvroClientMain {
  val logger = Logger()

  def main(args: Array[String]): Unit = {
    if(args.size == 3)
      try {
        val clusterRoot = ZooKeeperNode(args(1))
        Class.forName(args(0)).newInstance.asInstanceOf[AvroClient].parse(args(2)).run(clusterRoot)
      }
      catch {
        case error => {
          logger.fatal(error, "Exeception in Main Thread.  Killing process.")
          System.exit(-1)
      }
    }
    else {
      println("Usage: " + this.getClass.getName + "<class name> <zookeeper address> <json encoded avro client description>")
      System.exit(-1)
    }
  }
}
