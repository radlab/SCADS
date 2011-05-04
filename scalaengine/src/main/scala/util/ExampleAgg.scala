package edu.berkeley.cs.scads.storage.examples

import edu.berkeley.cs.scads.storage._
import edu.berkeley.cs.avro.marker.AvroRecord
import edu.berkeley.cs.scads.comm._
import edu.berkeley.cs.scads.util._

case class IntRec2(var x: Int) extends AvroRecord
case class TestCmp(var x:Int, var y:Int) extends AvroRecord

class := {
  def doPred(value:Comparable[Object], target:Comparable[Object]):Boolean = {
    (value.compareTo(target) == 0)
  }
}

object ExampleAgg {
  def main(args: Array[String]): Unit = {
    val cluster = TestScalaEngine.newScadsCluster()
    val ns = cluster.getNamespace[IntRec2, TestCmp]("testns")
    ns.put(IntRec2(1), TestCmp(1,2))
    ns.put(IntRec2(2), TestCmp(1,3))
    println("Received Record:" + ns.get(IntRec2(1)))

    val fm = new :=
    val filts = AggFilters(Array[Byte](4),List[AggFilter](AggFilter("y",fm.getClass.getName,AnalyticsUtils.getFunctionCode(fm))))
    
    ns.applyAggregate(List("foo","bar"),Option(filts))
  }
}
