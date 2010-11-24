package edu.berkeley.cs.scads.storage.examples

import edu.berkeley.cs.scads.storage._
import edu.berkeley.cs.avro.marker.AvroRecord

case class IntRec(var x: Int) extends AvroRecord

object Example {
  def main(args: Array[String]): Unit = {
    val cluster = TestScalaEngine.newScadsCluster()
    val ns = cluster.getNamespace[IntRec, IntRec]("testns")
    ns.put(IntRec(1), IntRec(2))
    println("Received Record:" + ns.get(IntRec(1)))
  }
}
