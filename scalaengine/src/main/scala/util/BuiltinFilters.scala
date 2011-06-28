package edu.berkeley.cs.scads.util

import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import edu.berkeley.cs.avro.marker.AvroRecord
import edu.berkeley.cs.avro.runtime.ScalaSpecificRecord

private trait Cmper {
  def cmp(rec:ScalaSpecificRecord, pos:Int, value:Any):Int
}

private object CmpGetter {
  def getCmper(t:Schema.Type):Cmper = {
    t match {
      case Schema.Type.INT => IntCmp
      case Schema.Type.LONG => LongCmp
      case Schema.Type.FLOAT => FloatCmp
      case Schema.Type.DOUBLE => DoubleCmp
      case Schema.Type.STRING => StrCmp
      case Schema.Type.BOOLEAN => BoolCmp
      case Schema.Type.NULL => NullCmp
      case Schema.Type.RECORD => RecCmp
      case _ => throw new Exception("Can't average non-numeric field")
    }
  }
}

private object NullCmp extends Cmper {
  def cmp(rec:ScalaSpecificRecord, pos:Int, value:Any):Int = {
    if (rec.get(pos) == null) 0
    else 1
  }
}
 
private object BoolCmp extends Cmper {
  def cmp(rec:ScalaSpecificRecord, pos:Int, value:Any):Int = {
    rec.get(pos).asInstanceOf[Boolean].compareTo(value.asInstanceOf[Boolean])
  }
}

private object IntCmp extends Cmper {
  def cmp(rec:ScalaSpecificRecord, pos:Int, value:Any):Int = {
    rec.get(pos).asInstanceOf[Int].compareTo(value.asInstanceOf[Int])
  }
}

private object LongCmp extends Cmper {
  def cmp(rec:ScalaSpecificRecord, pos:Int, value:Any):Int = {
    rec.get(pos).asInstanceOf[Long].compareTo(value.asInstanceOf[Long])
  }
}

private object FloatCmp extends Cmper {
  def cmp(rec:ScalaSpecificRecord, pos:Int, value:Any):Int = {
    rec.get(pos).asInstanceOf[Float].compareTo(value.asInstanceOf[Float])
  }
}

private object DoubleCmp extends Cmper {
  def cmp(rec:ScalaSpecificRecord, pos:Int, value:Any):Int = {
    rec.get(pos).asInstanceOf[Double].compareTo(value.asInstanceOf[Double])
  }
}

private object StrCmp extends Cmper {
  def cmp(rec:ScalaSpecificRecord, pos:Int, value:Any):Int = {
    rec.get(pos).asInstanceOf[String].compareTo(value.asInstanceOf[String])
  }
}

private object RecCmp extends Cmper {
  def cmp(rec:ScalaSpecificRecord, pos:Int, value:Any):Int = {
    rec.get(pos).asInstanceOf[GenericData.Record].compareTo(value.asInstanceOf[GenericData.Record])
  }
}

class :=(field:String, target:Any) extends Filter[ScalaSpecificRecord] {
  var pos = -1 // will be updated first time we see a record
  private var cmper:Cmper = null
  def applyFilter(rec: ScalaSpecificRecord): Boolean = {
    if (pos < 0) {
      val f = rec.getSchema.getField(field)
      pos = f.pos
      cmper = CmpGetter.getCmper(f.schema.getType)
    }
    cmper.cmp(rec,pos,target) == 0
  }
}

class :!=(field:String, target:Any) extends Filter[ScalaSpecificRecord] {
  var pos = -1 // will be updated first time we see a record
  private var cmper:Cmper = null
  def applyFilter(rec: ScalaSpecificRecord): Boolean = {
    if (pos < 0) {
      val f = rec.getSchema.getField(field)
      pos = f.pos
      cmper = CmpGetter.getCmper(f.schema.getType)
    }
    cmper.cmp(rec,pos,target) != 0
  }
}

class :>(field:String, target:Any) extends Filter[ScalaSpecificRecord] {
  var pos = -1 // will be updated first time we see a record
  private var cmper:Cmper = null
  def applyFilter(rec: ScalaSpecificRecord): Boolean = {
    if (pos < 0) {
      val f = rec.getSchema.getField(field)
      pos = f.pos
      cmper = CmpGetter.getCmper(f.schema.getType)
    }
    cmper.cmp(rec,pos,target) > 0
  }
}

class :<(field:String, target:Any) extends Filter[ScalaSpecificRecord] {
  var pos = -1 // will be updated first time we see a record
  private var cmper:Cmper = null
  def applyFilter(rec: ScalaSpecificRecord): Boolean = {
    if (pos < 0) {
      val f = rec.getSchema.getField(field)
      pos = f.pos
      cmper = CmpGetter.getCmper(f.schema.getType)
    }
    cmper.cmp(rec,pos,target) < 0
  }
}

class :<=(field:String, target:Any) extends Filter[ScalaSpecificRecord] {
  var pos = -1 // will be updated first time we see a record
  private var cmper:Cmper = null
  def applyFilter(rec: ScalaSpecificRecord): Boolean = {
    if (pos < 0) {
      val f = rec.getSchema.getField(field)
      pos = f.pos
      cmper = CmpGetter.getCmper(f.schema.getType)
    }
    cmper.cmp(rec,pos,target) <= 0
  }
}

class :>=(field:String, target:Any) extends Filter[ScalaSpecificRecord] {
  var pos = -1 // will be updated first time we see a record
  private var cmper:Cmper = null
  def applyFilter(rec: ScalaSpecificRecord): Boolean = {
    if (pos < 0) {
      val f = rec.getSchema.getField(field)
      pos = f.pos
      cmper = CmpGetter.getCmper(f.schema.getType)
    }
    cmper.cmp(rec,pos,target) >= 0
  }
}
