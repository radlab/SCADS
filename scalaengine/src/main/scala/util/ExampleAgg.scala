package edu.berkeley.cs.scads.storage.examples

import edu.berkeley.cs.scads.storage._
import edu.berkeley.cs.avro.marker.AvroRecord
import edu.berkeley.cs.scads.comm._
import edu.berkeley.cs.scads.util._

case class IntRec2(var x: Int) extends AvroRecord
case class MultiTestRec(var f1:Int, var f2:Int, var f3:Int) extends AvroRecord

case class BasicAggContainer(var curcount:Int, var curval:Int) extends AvroRecord

class := {
  def doPred(value:Comparable[Object], target:Comparable[Object]):Boolean = {
    (value.compareTo(target) == 0)
  }
}

class CompAvg {
  def doAgg(ag:BasicAggContainer, key:IntRec2, value: MultiTestRec):BasicAggContainer = {
    ag.curcount += 1
    ag.curval = ag.curval + ((value.f1 - ag.curval) / ag.curcount)
    ag
  }
}

object ExampleAgg {

  def localAvg(cur:BasicAggContainer, next:BasicAggContainer):BasicAggContainer = {
    cur.curcount += next.curcount
    cur.curval = cur.curval + (next.curval * next.curcount)
    cur
  }

  def main(args: Array[String]): Unit = {
    val cluster = TestScalaEngine.newScadsCluster()
    val ns = cluster.getNamespace[IntRec2, MultiTestRec]("testns")
    ns.put(IntRec2(1), MultiTestRec(1,2,3))
    ns.put(IntRec2(2), MultiTestRec(2,2,2))
    ns.put(IntRec2(3), MultiTestRec(0,2,0))
    println("Received Record:" + ns.get(IntRec2(1)))

    val initer = ()=>{BasicAggContainer(0,0)}
    val initerBytes = AnalyticsUtils.getClassBytes(initer)
    val initerName = initer.getClass.getName

    val aggName = classOf[CompAvg].getName
    val aggBytes = AnalyticsUtils.getClassBytes(new CompAvg)

    val aggOp = AggOp(initerName,initerBytes, aggName, aggBytes, false)

    val fm = new :=
    val filts = AggFilters(Array[Byte](4),List[AggFilter](AggFilter("f2",fm.getClass.getName,AnalyticsUtils.getClassBytes(fm))))
    
    ns.applyAggregate(List[String]("f1"),classOf[IntRec2].getName,classOf[MultiTestRec].getName,Option(filts),List[AggOp](aggOp),new BasicAggContainer(0,0))
  }
}
