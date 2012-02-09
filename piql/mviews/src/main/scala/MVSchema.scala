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
  val suffix = "results6"
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

case class ParResult(
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
  var failures: Int = _
  var readFrac: Double = _

  def fmt: String = {
      val x = ("clientId=" + clientId,
               "itemsPerMachine=" + itemsPerMachine,
               "readFrac=" + readFrac,
               "l_get=" + getTimes.quantile(0.5),
               "l_put=" + putTimes.quantile(0.5),
               "l_del=" + delTimes.quantile(0.5),
               "ops/s=" + (getTimes.totalRequests + putTimes.totalRequests + delTimes.totalRequests)*1.0/runTimeMs*1000,
               "nClients=" + nClients,
               "partitions=" + partitions,
               "threads=" + threadCount)
      val s = x.toString
      var s2 = s.replaceAll(",","\t").substring(1, s.length-1)
      if (failures > 0) {
        s2 += "\tFAILURES=" + failures
      }
      return s2
  }
}
