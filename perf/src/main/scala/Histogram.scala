package edu.berkeley.cs.scads.perf

import edu.berkeley.cs.avro.runtime._
import edu.berkeley.cs.avro.marker._

import scala.collection.mutable.ArrayBuffer
import scala.xml._

object Histogram {
  def apply(bucketSize: Int, bucketCount: Int): Histogram =
    new Histogram(bucketSize, ArrayBuffer.fill(bucketCount)(0L))
}

case class Histogram(var bucketSize: Int, var buckets: ArrayBuffer[Long]) extends AvroRecord {
  def +(left: Histogram): Histogram = {
    require(bucketSize == left.bucketSize)
    require(buckets.size == left.buckets.size)

    Histogram(bucketSize, buckets.zip(left.buckets).map{ case (a,b) => a + b })
  }

  def +=(value: Long) {
    add(value)
  }

	def add(value: Long):Histogram = {
		val bucket = (value / bucketSize).toInt
		if(bucket >= buckets.length)
			buckets(buckets.length - 1) += 1
		else
			buckets(bucket) +=1

		this
	}

  def totalRequests: Long = buckets.sum

  def quantile(fraction: Double): Int = {
    val cumulativeSum = buckets.scanLeft(0L)(_ + _).drop(1)
    cumulativeSum.findIndexOf(_ >= totalRequests * fraction) * bucketSize
  }

  def average: Double = {
    val n = totalRequests.toDouble
    buckets.zipWithIndex.foldLeft(0.0) { case (acc, (num, idx)) => acc + num.toDouble * idx.toDouble * bucketSize.toDouble / n }
  }

  /**
   * Sample standard deviation based off of sqrt(1/(N-1)*sum((Xi-mean(X))^2))
   */
  def stddev: Double = {
    val n = totalRequests.toDouble
    val xbar = average
    import scala.math._
    sqrt(1.0 / (n - 1.0) * buckets.zipWithIndex.foldLeft(0.0) { case (acc, (num, idx)) => acc + num.toDouble * pow(idx.toDouble * bucketSize.toDouble - xbar, 2) })
  }

	def view: NodeSeq =
<script type="text/javascript">{"""
 $(document).ready(function() {
      var chart1 = new Highcharts.Chart({
         chart: {
            renderTo: 'chart',
            defaultSeriesType: 'bar'
         },
         title: {
            text: 'Histogram'
         },
         xAxis: {
            categories: """ + (1 to buckets.length).map(i => {"'" + i * buckets.length +"'"}).mkString("[", ",", "]") + """
         },
         yAxis: {
            title: {
               text: 'BucketCounts'
            }
         },
         series: [{
            data: """ + buckets.mkString("[", ",", "]") + """
         }]
      });
   });"""}
</script>
}
