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