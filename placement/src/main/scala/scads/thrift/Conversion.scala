package edu.berkeley.cs.scads.thrift

import edu.berkeley.cs.scads.keys._
import edu.berkeley.cs.scads.keys.AutoKey
import java.text.ParsePosition

trait Conversions extends AutoKey {
  implicit def pairToRecord(p: (String ,String)): Record =
    new Record(p._1, p._2)

  implicit def recordToPair(r: Record): (String ,String) = {
    val p = new ParsePosition(0)
    (r.key, r.value)
  }
}

trait RangeConversion {
	def keyRangeToScadsRangeSet(kr:KeyRange):RecordSet = {
		val recSet = new RecordSet
		val range = new RangeSet
		recSet.setType(RecordSetType.RST_RANGE)
		recSet.setRange(range)
		range.setStart_key(kr.start.serialize)
		range.setEnd_key(kr.end.serialize)
		return recSet
	}
}
