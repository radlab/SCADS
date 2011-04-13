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

    Histogram(bucketSize, buckets.zip(left.buckets).map { case (a, b) => a + b })
  }

  def reset(): Histogram = synchronized {
    val oldHist = buckets
    buckets = ArrayBuffer.fill(oldHist.size)(0L)
    Histogram(bucketSize, oldHist)
  }

  def +=(value: Long) {
    add(value)
  }

  def add(value: Long): Histogram = synchronized {
    val bucket = (value / bucketSize).toInt
    if (bucket >= buckets.length)
      buckets(buckets.length - 1) += 1
    else
      buckets(bucket) += 1

    this
  }

  def totalRequests: Long = buckets.sum

  def quantile(fraction: Double): Int = {
    val cumulativeSum = buckets.scanLeft(0L)(_ + _).drop(1)
    if(totalRequests > 0)
      cumulativeSum.findIndexOf(_ >= totalRequests * fraction) * bucketSize
    else
      0
  }

  def median: Int = {
    quantile(0.5)
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

  def toHtml: NodeSeq =
    <script type="text/javascript">
      {
        """
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
            categories: """ + (1 to buckets.length).map(i => { "'" + i * buckets.length + "'" }).mkString("[", ",", "]") + """
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

	override def toString: String = {
		getBucketCounts
	}
	
	def getBucketEndpoints: String = {
		(1 to buckets.length).map(i => {"'" + i * bucketSize +"'"}).mkString("[", ",", "]")
	}
	
	def getBucketCounts: String = {
		buckets.mkString(",")
	}
	
	def sameLength(that: Histogram): Boolean = {
	  this.buckets.lengthCompare(that.buckets.length) == 0
	}
	

	// TODO: write function to decrease the resolution from M to N
	// NOTE: M must be divisible by N (eg, 1000, 100)
	
	


  /* confirmed that result is same as that from http://www.ece.unm.edu/signals/signals/Discrete_Convolution/discrete_convolution.html */
	def convolveWith(that: Histogram): Histogram = {
	  var defaultBucketSize = 1
	  var defaultBucketCount = 1000
	  assert(this.buckets.length == defaultBucketCount, "this must have " + defaultBucketCount + " buckets")
	  assert(that.buckets.length == defaultBucketCount, "that must have " + defaultBucketCount + " buckets")
	  
	  val result = Histogram(defaultBucketSize, defaultBucketCount)
	  
	  (0 to defaultBucketCount-1).foreach(i => {
	    var integralOfOverlap = 0L

      (0 to i).foreach(j => {
        integralOfOverlap += this.buckets(i-j) * that.buckets(j)
      })

      result.buckets(i) = integralOfOverlap
	  })
	  
	  result
	}
	
	def histToCDF() : Histogram = {
	  var defaultBucketSize = 1
	  var defaultBucketCount = 10
	  val intermResult = Histogram(defaultBucketSize, defaultBucketCount)
	  var bucketTot : Double = 0
	  var prob : Double = 0
	  
	  assert(this.buckets.length == defaultBucketCount, "histogram must have " + defaultBucketCount + " buckets")
	  
	  /*Convert histogram to probability histogram*/
	  var max = defaultBucketCount-1
	  
	  (0 to max).foreach(i => {
	    bucketTot = (this.buckets(i)).toDouble
	    //System.out.println("Total in bucket "+ bucketTot)
	    //System.out.println("TotalRequests "+ this.totalRequests)
	    prob = (bucketTot/((this.totalRequests).toDouble))*100
	    //System.out.println("Prob value " + prob)
	    intermResult.buckets(i) = prob.toLong
	  })

	  /*Using probability histogram, create CDF*/
	  val cdfResult = Histogram(defaultBucketSize, defaultBucketCount)
	  var totSum : Long = 0
	  (0 to max).foreach(i=> {
	    totSum = 0
	    (0 to i).foreach(j=> {
	      totSum = intermResult.buckets(j) + totSum
	    })
	    cdfResult.buckets(i) = totSum
	  })
	 
	  cdfResult
	}
	
  def collapseHist(newBucketCount : Int): Histogram = {
    var defaultBucketSize = 1
    var originalHistLength = this.buckets.length    
    var total = 0L
    
    assert(this.buckets.length%newBucketCount == 0, "The original bucket count (" +this.buckets.length+") must be divisible by the new bucket count.")
    val collapseAmt = originalHistLength/newBucketCount
    System.out.println("Collapse value is: " + collapseAmt)

    val collapsedHistogram = Histogram(defaultBucketSize, newBucketCount)
    var max = newBucketCount-1
    var collapsedHistogramIndex = 0
    var origIndex = 0
    var collapseVal = collapseAmt

    while(collapsedHistogramIndex <= max)
    {
    	while (origIndex < collapseVal)
    	{
    		total = this.buckets(origIndex)+total
    		origIndex = origIndex+1
    	}
    	collapsedHistogram.buckets(collapsedHistogramIndex) = total
    	collapseVal = collapseVal + collapseAmt
    	total = 0
    	collapsedHistogramIndex = collapsedHistogramIndex + 1
    }
        
    collapsedHistogram
  }
}
