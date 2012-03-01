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
case class MTagPair(var tag1: String,
                      var tag2: String,
                      var item: String) extends AvroPair {
}

object Results {
  val suffix = "results9"
}

case class MVResult(
  var hostname: String,
  var clientId: String,
  var iteration: Int,
  var scale: Int,
  var threadCount: Int) extends AvroPair {

  var timestamp: Long = _
  var loadTimeMs: Long = _
  var runTimeMs: Long =  _
  var responseTimes: Histogram = null
  var failures: Int = _

  def fmt: String = {
      val x = ("clientId=" + clientId,
               "scale=" + scale,
               "lat(0.5)=" + responseTimes.quantile(0.5),
               "lat(0.99)=" + responseTimes.quantile(0.99),
               "gets/s=" + responseTimes.totalRequests*1.0/runTimeMs*1000,
               "threads=" + threadCount)
      val s = x.toString
      return s.replaceAll(",","\t").substring(1, s.length-1)
  }
}

case class ParResult3(
  var timestamp: Long,
  var hostname: String,
  var iteration: Int,
  var clientId: String) extends AvroPair {

  var threadCount: Int = _
  var clientNumber: Int = _
  var nClients: Int = _
  var replicas: Int = _
  var partitions: Int = _
  var itemsPerMachine: Int = _
  var maxTags: Int = _
  var meanTags: Int = _
  var loadTimeMs: Long = _
  var runTimeMs: Long =  _
  var putTimes: Histogram = null
  var getTimes: Histogram = null
  var delTimes: Histogram = null
  var nvputTimes: Histogram = null
  var nvdelTimes: Histogram = null
  var failures: Int = _
  var readFrac: Double = _
  var comment: String = _

  def timeSince(): String = {
    val dt = System.currentTimeMillis - timestamp
    if (dt < 2 * 1000 * 60) {
      "%d seconds ago".format(dt/1000)
    } else if (dt < 2 * 1000 * 60 * 60) {
      "%d minutes ago".format(dt/1000/60)
    } else if (dt < 2 * 1000 * 60 * 60 * 24) {
      "%d hours ago".format(dt/1000/60/60)
    } else {
      "%d days ago".format(dt/1000/60/60/24)
    }
  }

  def fmt: String = {
      val x = (timeSince(),
               "" + clientId,
               (clientNumber+1) + "/" + nClients,
               "rf=" + readFrac,
               "get=%.1f/%.1f".format(getTimes.quantile(0.5)/1000.0, getTimes.quantile(0.99)/1000.0),
               "put=%.1f/%.1f".format(putTimes.quantile(0.5)/1000.0, putTimes.quantile(0.99)/1000.0),
//               "del=%.1f/%.1f".format(delTimes.quantile(0.5)/1000.0, delTimes.quantile(0.99)/1000.0),
//               "nvput=%.1f/%.1f".format(nvputTimes.quantile(0.5)/1000.0, nvputTimes.quantile(0.99)/1000.0),
//               "nvdel=%.1f/%.1f".format(nvdelTimes.quantile(0.5)/1000.0, nvdelTimes.quantile(0.99)/1000.0),
               "ops/s=" + (getTimes.totalRequests + putTimes.totalRequests + delTimes.totalRequests)*1.0/runTimeMs*1000,
               "rep=" + replicas,
               "par=" + partitions,
               "threads=" + threadCount,
               "items/m=" + itemsPerMachine,
               comment)
      val s = x.toString
      var s2 = s.replaceAll(","," ").substring(1, s.length-1)
      if (failures > 0) {
        s2 += " FAILURES=" + failures
      }
      return s2
  }
}
