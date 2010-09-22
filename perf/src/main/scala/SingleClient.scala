package edu.berkeley.cs.scads.perf

import edu.berkeley.cs.scads.comm._
import edu.berkeley.cs.scads.mesos._

import edu.berkeley.cs.avro.runtime._

object SingleClient extends optional.Application {
  implicit val zooRoot = RClusterZoo.root

  def main(numRecords: Int = 1000): Unit = {
    println("Starting test with " + numRecords + " records")
    val cluster = ScadsMesosCluster()
    Thread.sleep(10000)

    val ns = cluster.getNamespace[IntRec, IntRec]("singleclientperftests")

    println("Starting put of " + numRecords + " records.")
    ns ++= (1 to numRecords).map(i => (IntRec(i), IntRec(i*2)))

    println("Starting gets of " + numRecords + " records.")
    (1 to numRecords).foreach(i => ns.get(IntRec(i)))

    println("Done!")
    System.exit(0)
  }
}
