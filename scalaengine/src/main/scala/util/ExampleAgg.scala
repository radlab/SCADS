package edu.berkeley.cs.scads.storage.examples

import edu.berkeley.cs.scads.storage._
import edu.berkeley.cs.avro.marker.AvroRecord
import edu.berkeley.cs.scads.comm._
import edu.berkeley.cs.scads.util._

case class IntRec2(var x: Int) extends AvroRecord
case class MultiTestRec(var f1: Int, var f2: Int, var f3: Int) extends AvroRecord

object ExampleAgg {

  def main(args: Array[String]): Unit = {
    val cluster = TestScalaEngine.newScadsCluster()
    val ns = cluster.getNamespace[IntRec2, MultiTestRec]("testns")
    ns.put(IntRec2(1), MultiTestRec(7, 2, 3))
    ns.put(IntRec2(2), MultiTestRec(2, 2, 2))
    ns.put(IntRec2(3), MultiTestRec(3, 2, 0))
    ns.put(IntRec2(4), MultiTestRec(13, 3, 0))
    ns.put(IntRec2(5), MultiTestRec(11, 3, 0))

    val agg = ns.applyAggregate(List[String]("f2"),
                                classOf[IntRec2].getName,
                                classOf[MultiTestRec].getName,
                                List(new :>("f3",0)),
                                List((new AvgLocal,new AvgRemote("f1"))))
    println(agg)

    ns.put(IntRec2(6), MultiTestRec(15, 3, 0))


    val aggm = ns.applyAggregate(List[String]("f2"),
                                 classOf[IntRec2].getName,
                                 classOf[MultiTestRec].getName,
                                 List(),
                                 List(
                                   (new MaxLocal,new MaxRemote("f1")),
                                   (new MinLocal,new MinRemote("f1")),
                                   (new SumLocal,new SumRemote("f1"))
                                 )
                               )
    println(aggm)

    val aggl = ns.applyAggregate(List[String](),
                                 classOf[IntRec2].getName,
                                 classOf[MultiTestRec].getName,
                                 List(),
                                 List(
                                   (new SampleAvgLocal,new SampleAvgRemote("f1",2,true)),
                                   (new SampleAvgLocal,new SampleAvgRemote("f1",2)),
                                   (new SampleAvgLocal,new SampleAvgRemote("f1",3))
                                 )
                               )
    println(aggl)
  }
}
