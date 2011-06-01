package edu.berkeley.cs.scads.util
import org.apache.avro.Schema
import edu.berkeley.cs.avro.marker.AvroRecord
import edu.berkeley.cs.avro.runtime.ScalaSpecificRecord

case class BasicAggContainer(var curcount: Int, var curval: Double) extends AvroRecord

// average
sealed trait AvgRunner {
  def runAvg(ag: BasicAggContainer, value:ScalaSpecificRecord, pos:Int):BasicAggContainer
}

private object IntRunner extends AvgRunner {
  def runAvg(ag: BasicAggContainer, value:ScalaSpecificRecord, pos:Int):BasicAggContainer = {
    ag.curcount += 1
    ag.curval = ag.curval + ((value.get(pos).asInstanceOf[Int] - ag.curval) / ag.curcount)
    ag    
  }    
}

private object LongRunner extends AvgRunner {
  def runAvg(ag: BasicAggContainer, value:ScalaSpecificRecord, pos:Int):BasicAggContainer = {
    ag.curcount += 1
    ag.curval = ag.curval + ((value.get(pos).asInstanceOf[Long] - ag.curval) / ag.curcount)
    ag    
  }    
}

private object FloatRunner extends AvgRunner {
  def runAvg(ag: BasicAggContainer, value:ScalaSpecificRecord, pos:Int):BasicAggContainer = {
    ag.curcount += 1
    ag.curval = ag.curval + ((value.get(pos).asInstanceOf[Float] - ag.curval) / ag.curcount)
    ag    
  }    
}

private object DoubleRunner extends AvgRunner {
  def runAvg(ag: BasicAggContainer, value:ScalaSpecificRecord, pos:Int):BasicAggContainer = {
    ag.curcount += 1
    ag.curval = ag.curval + ((value.get(pos).asInstanceOf[Double] - ag.curval) / ag.curcount)
    ag    
  }    
}

class AvgRemote(field:String) extends RemoteAggregate[BasicAggContainer, ScalaSpecificRecord, ScalaSpecificRecord] {
  var pos = -1 // will be updated first time we see a record
  private var runner:AvgRunner = null

  def init(): BasicAggContainer = {
    BasicAggContainer(0, 0)
  }

  def applyAggregate(ag: BasicAggContainer, key: ScalaSpecificRecord, value: ScalaSpecificRecord): BasicAggContainer = {
    if (pos < 0) {
      pos = value.getSchema.getField(field).pos
      runner = 
        value.getSchema.getField(field).schema.getType match {
          case Schema.Type.INT => IntRunner
          case Schema.Type.LONG => LongRunner
          case Schema.Type.FLOAT => FloatRunner
          case Schema.Type.DOUBLE => DoubleRunner
          case _ => throw new Exception("Can't average non-numeric field")
        }
    }
    runner.runAvg(ag,value,pos)
  }
}

class AvgLocal extends LocalAggregate[BasicAggContainer, Double] {
  def init(): BasicAggContainer = {
    BasicAggContainer(0, 0)
  }
  def foldFunction(cur: BasicAggContainer, next: BasicAggContainer): BasicAggContainer = {
    cur.curcount += next.curcount
    cur.curval = cur.curval + (next.curval * next.curcount)
    cur
  }
  def finalize(f: BasicAggContainer): Double = {
    f.curval.asInstanceOf[Double] / f.curcount
  }
}
