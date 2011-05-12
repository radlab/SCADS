package edu.berkeley.cs.scads.storage.examples

import edu.berkeley.cs.scads.storage._
import edu.berkeley.cs.avro.marker.AvroRecord
import edu.berkeley.cs.scads.comm._
import edu.berkeley.cs.scads.util._

case class IntRec2(var x: Int) extends AvroRecord
case class MultiTestRec(var f1:Int, var f2:Int, var f3:Int) extends AvroRecord

case class BasicAggContainer(var curcount:Int, var curval:Int) extends AvroRecord

class := extends Filter[MultiTestRec] {

  var target:MultiTestRec = null

  def applyFilter(rec:MultiTestRec):Boolean = {
    rec.get(field) == target.get(field)
  }
}

class CompAvg extends Aggregate[BasicAggContainer,IntRec2,MultiTestRec] {

  def init():BasicAggContainer = {
    BasicAggContainer(0,0)
  }

  def applyAggregate(ag:BasicAggContainer, key:IntRec2, value: MultiTestRec):BasicAggContainer = {
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

    val trec = MultiTestRec(0,2,0)
    val fm = new :=
    fm.init(1,trec)

    val agg = new CompAvg
    
    ns.applyAggregate(List[String]("f1"),
                      classOf[IntRec2].getName,
                      classOf[MultiTestRec].getName,
                      List(fm),
                      List(agg),
                      new BasicAggContainer(0,0))
  }
}
