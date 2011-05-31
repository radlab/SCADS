package edu.berkeley.cs.scads.storage.examples

import edu.berkeley.cs.scads.storage._
import edu.berkeley.cs.avro.marker.AvroRecord
import edu.berkeley.cs.scads.comm._
import edu.berkeley.cs.scads.util._

import edu.berkeley.cs.avro.runtime.ScalaSpecificRecord

case class IntRec2(var x: Int) extends AvroRecord
case class MultiTestRec(var f1: Int, var f2: Int, var f3: Int) extends AvroRecord

case class BasicAggContainer(var curcount: Int, var curval: Float) extends AvroRecord

class := extends Filter[MultiTestRec] {

  var target: MultiTestRec = null

  def applyFilter(rec: MultiTestRec): Boolean = {
    rec.get(field) == target.get(field)
  }
}

object CompAvg extends Aggregate[BasicAggContainer, ScalaSpecificRecord, MultiTestRec, Float] {
  class Remote extends RemoteAggregate[BasicAggContainer, ScalaSpecificRecord, MultiTestRec] {
    def init(): BasicAggContainer = {
      BasicAggContainer(0, 0)
    }

    def applyAggregate(ag: BasicAggContainer, key: ScalaSpecificRecord, value: MultiTestRec): BasicAggContainer = {
      ag.curcount += 1
      ag.curval = ag.curval + ((value.f1 - ag.curval) / ag.curcount)
      ag
    }
  }

  class Local extends LocalAggregate[BasicAggContainer, Float] {
    def init(): BasicAggContainer = {
      BasicAggContainer(0, 0)
    }
    def foldFunction(cur: BasicAggContainer, next: BasicAggContainer): BasicAggContainer = {
      cur.curcount += next.curcount
      cur.curval = cur.curval + (next.curval * next.curcount)
      cur
    }
    def finalize(f: BasicAggContainer): Float = {
      f.curval.asInstanceOf[Float] / f.curcount
    }
  }

  val remoteAggregate = new Remote
  val localAggregate = new Local
}

object ExampleAgg {

  def localAvg(cur: BasicAggContainer, next: BasicAggContainer): BasicAggContainer = {
    cur.curcount += next.curcount
    cur.curval = cur.curval + (next.curval * next.curcount)
    cur
  }

  def main(args: Array[String]): Unit = {
    val cluster = TestScalaEngine.newScadsCluster()
    val ns = cluster.getNamespace[IntRec2, MultiTestRec]("testns")
    ns.put(IntRec2(1), MultiTestRec(1, 2, 3))
    ns.put(IntRec2(2), MultiTestRec(2, 2, 2))
    ns.put(IntRec2(3), MultiTestRec(3, 2, 0))
    ns.put(IntRec2(4), MultiTestRec(13, 3, 0))

    val trec = MultiTestRec(0, 2, 0)
    val fm = new :=
    fm.init(1, trec)

    //val agg = new CompAvg

    val agg = ns.applyAggregate(List[String]("f2"),
                                classOf[IntRec2].getName,
                                classOf[MultiTestRec].getName,
                                List(),
                                List(CompAvg))
    println(agg)
  }
}
