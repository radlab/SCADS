package edu.berkeley.cs.scads.util
import org.apache.avro.Schema
import edu.berkeley.cs.avro.marker.AvroRecord
import edu.berkeley.cs.avro.runtime.ScalaSpecificRecord

case class ValueAggContainer(var curval: Double) extends AvroRecord
case class CountAggContainer(var curcount: Int, var curval: Double) extends AvroRecord

// average
class AvgRemote(field:String) extends RemoteAggregate[CountAggContainer, ScalaSpecificRecord, ScalaSpecificRecord] {
  var pos = -1 // will be updated first time we see a record

  def init(): CountAggContainer = {
    CountAggContainer(0, 0)
  }

  def applyAggregate(ag: CountAggContainer, key: ScalaSpecificRecord, value: ScalaSpecificRecord): CountAggContainer = {
    if (pos < 0)
      pos = value.getSchema.getField(field).pos
    ag.curcount += 1
    ag.curval = ag.curval + ((toDouble(value.get(pos)) - ag.curval) / ag.curcount)
    ag    
  }
}

class AvgLocal extends LocalAggregate[CountAggContainer, Double] {
  def init(): CountAggContainer = {
    CountAggContainer(0, 0)
  }
  def foldFunction(cur: CountAggContainer, next: CountAggContainer): CountAggContainer = {
    cur.curcount += next.curcount
    cur.curval = cur.curval + (next.curval * next.curcount)
    cur
  }
  def finalize(f: CountAggContainer): Double = {
    f.curval / f.curcount
  }
}


// max
class MaxRemote(field:String) extends RemoteAggregate[ValueAggContainer, ScalaSpecificRecord, ScalaSpecificRecord] {
  var pos = -1 // will be updated first time we see a record

  def init(): ValueAggContainer = {
    ValueAggContainer(java.lang.Double.MIN_VALUE)
  }

  def applyAggregate(ag: ValueAggContainer, key: ScalaSpecificRecord, value: ScalaSpecificRecord): ValueAggContainer = {
    if (pos < 0)
      pos = value.getSchema.getField(field).pos
    val v = toDouble(value.get(pos))
    if (v > ag.curval)
      ag.curval = v
    ag
  }
}

class MaxLocal extends LocalAggregate[ValueAggContainer, Double] {
  def init(): ValueAggContainer = {
    ValueAggContainer(java.lang.Double.MIN_VALUE)
  }
  def foldFunction(cur: ValueAggContainer, next: ValueAggContainer):ValueAggContainer = {
    if (cur.curval < next.curval)
      cur.curval = next.curval
    cur
  }
  def finalize(f: ValueAggContainer):Double = {
    f.curval
  }
}


// min
class MinRemote(field:String) extends RemoteAggregate[ValueAggContainer, ScalaSpecificRecord, ScalaSpecificRecord] {
  var pos = -1 // will be updated first time we see a record

  def init(): ValueAggContainer = {
    ValueAggContainer(java.lang.Double.MAX_VALUE)
  }

  def applyAggregate(ag: ValueAggContainer, key: ScalaSpecificRecord, value: ScalaSpecificRecord): ValueAggContainer = {
    if (pos < 0)
      pos = value.getSchema.getField(field).pos
    val v = toDouble(value.get(pos))
    if (v < ag.curval)
      ag.curval = v
    ag
  }
}

class MinLocal extends LocalAggregate[ValueAggContainer, Double] {
  def init(): ValueAggContainer = {
    ValueAggContainer(java.lang.Double.MAX_VALUE)
  }
  def foldFunction(cur: ValueAggContainer, next: ValueAggContainer):ValueAggContainer = {
    if (cur.curval > next.curval)
      cur.curval = next.curval
    cur
  }
  def finalize(f: ValueAggContainer):Double = {
    f.curval
  }
}



// sum
class SumRemote(field:String) extends RemoteAggregate[ValueAggContainer, ScalaSpecificRecord, ScalaSpecificRecord] {
  var pos = -1 // will be updated first time we see a record

  def init(): ValueAggContainer = {
    ValueAggContainer(0)
  }

  def applyAggregate(ag: ValueAggContainer, key: ScalaSpecificRecord, value: ScalaSpecificRecord): ValueAggContainer = {
    if (pos < 0)
      pos = value.getSchema.getField(field).pos
    ag.curval += toDouble(value.get(pos))
    ag
  }
}

class SumLocal extends LocalAggregate[ValueAggContainer, Double] {
  def init(): ValueAggContainer = {
    ValueAggContainer(0)
  }
  def foldFunction(cur: ValueAggContainer, next: ValueAggContainer):ValueAggContainer = {
    cur.curval += next.curval
    cur
  }
  def finalize(f: ValueAggContainer):Double = {
    f.curval
  }
}




// average over limited number of records
// if hard is true, EXACTLY limit records will be averaged, 
// otherwise, AT LEAST limit records will be averaged
class SampleAvgRemote(field:String,limit:Int,hard:Boolean=false) extends RemoteAggregate[CountAggContainer, ScalaSpecificRecord, ScalaSpecificRecord] {
  var pos = -1 // will be updated first time we see a record

  def init(): CountAggContainer = {
    CountAggContainer(0, 0)
  }

  /* -- some internal private vars for maintaining error counts -- */
  // for welford's variance
  var Mk:Double = 0.0
  var Mkp:Double = 0.0
  var Sk:Double = 0.0
  var Skp:Double = 0.0

  var welvar:Double = 0.0
  var stderr:Double = 0.0

  def applyAggregate(ag: CountAggContainer, key: ScalaSpecificRecord, value: ScalaSpecificRecord): CountAggContainer = {
    ag.curcount += 1
    if (ag.curcount > limit) {
      stop = true
      if (hard) return ag
    }
    if (pos < 0) {
      pos = value.getSchema.getField(field).pos
      Mk = toDouble(value.get(pos))
      Mkp = Mk
      Skp = 0.0
    } else {
      // compute sample variance and mean using welford's method 
      val x = toDouble(value.get(pos))
      Mk = Mkp + (x - Mkp)/ag.curcount;
      Sk = Skp + (x - Mkp)*(x - Mk);
    
      Mkp = Mk
      Skp = Sk

      if (ag.curcount > 1) {
        welvar =  Sk/(ag.curcount - 1)
        stderr = welvar / scala.math.sqrt(ag.curcount)
      }
    }
    ag.curval = Mk
    ag
  }
}

class SampleAvgLocal extends LocalAggregate[CountAggContainer, Double] {
  def init(): CountAggContainer = {
    CountAggContainer(0, 0)
  }
  def foldFunction(cur: CountAggContainer, next: CountAggContainer): CountAggContainer = {
    cur.curcount += next.curcount
    cur.curval = cur.curval + (next.curval * next.curcount)
    cur
  }
  def finalize(f: CountAggContainer): Double = {
    f.curval / f.curcount
  }
}
