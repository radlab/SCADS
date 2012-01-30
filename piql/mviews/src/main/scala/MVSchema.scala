package edu.berkeley.cs
package scads
package piql
package mviews

import edu.berkeley.cs.scads.storage._
import edu.berkeley.cs.avro.marker._
import perf._

import org.apache.avro.util._

case class Tag(var word: String, var item: String) extends AvroPair {
}

/**
 * for materialized view maintained by MTagClient
 * invariants
 *   sorted (tag1, tag2) == (tag1, tag2)
 *   exists (tag1, item)
 *   exists (tag2, item)
 */
case class M_Tag_Pair(var tag1: String,
                      var tag2: String,
                      var item: String) extends AvroPair {
}

case class MVResult(
  var hostname: String,
  var clientId: String,
  var iteration: Int,
  var scale: Int,
  var tag_item_ratio: Double,
  var threadCount: Int) extends AvroPair {

  var timestamp: Long = _
  var loadTimeMs: Long = _
  var runTimeMs: Long =  _
  var responseTimes: Histogram = null
  var failures: Int = _
}
