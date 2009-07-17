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
	implicit def keyRangeToScadsRangeSet(kr:KeyRange):RecordSet = {
		val recSet = new RecordSet
		recSet.setType(RecordSetType.RST_RANGE)
		recSet.setRange(keyRangeToRangeSet(kr))
		return recSet
	}
	implicit def rangeSetToKeyRange(rs:RangeSet):KeyRange = {
		val start = if (rs.start_key==null) { MinKey } else { StringKey.deserialize(rs.start_key,new ParsePosition(0)) }
		val end = if (rs.end_key==null) { MaxKey } else { StringKey.deserialize(rs.end_key,new ParsePosition(0)) }
		new KeyRange( start,end )
	}
	def keyRangeToRangeSet(kr: KeyRange): RangeSet = {
		val range = new RangeSet
		val start = if (kr.start==MinKey) {null} else {kr.start.serialize}
		val end = if (kr.end==MaxKey) {null} else {kr.end.serialize}
		range.setStart_key(start)
		range.setEnd_key(end)
		range
	}
}
