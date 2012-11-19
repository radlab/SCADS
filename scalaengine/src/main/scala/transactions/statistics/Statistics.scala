package edu.berkeley.cs.scads.storage
package transactions

import _root_.edu.berkeley.cs.avro.marker.AvroRecord
import scala.math.{min, max}
import collection.mutable.{ArrayBuffer, ArraySeq}

object AccessStatsConstants {
  // In milliseconds.
//  val bucketSize = 60000
//  val numBuckets = 3
  val bucketSize = 10000
  val numBuckets = 10

  // alpha for EMA.
  val emaAlpha = 0.50
}

case class AccessStatsBucket(var bucket: Long, var num: Int) extends AvroRecord

case class Statistics(var buckets: Seq[AccessStatsBucket]) extends AvroRecord {
  // exponential moving average.
  def getEMA(alpha: Double = AccessStatsConstants.emaAlpha) = {
    val nowBucket = System.currentTimeMillis / AccessStatsConstants.bucketSize
    val currentFraction = (System.currentTimeMillis - nowBucket).toDouble / AccessStatsConstants.bucketSize

    var lastBucket:Long = 0
    var ema = -1.0

    val it = buckets.reverseIterator
    while (it.hasNext) {
      val b = it.next
      if (ema < 0) {
        // first time through.
        ema = b.num
        lastBucket = b.bucket
      } else if (b.bucket < nowBucket) {
        // apply ema.
        // ignore now bucket, since it is not full.
        val exp = (b.bucket - lastBucket)
        val factor = scala.math.pow(1.0 - alpha, exp)
        ema = alpha * b.num + factor * ema

        lastBucket = b.bucket
      }
    }

    if (ema < 0) ema = 0

    // TODO: take care of recent empty buckets.

    ema
  }

  def getMean(alpha: Double = AccessStatsConstants.emaAlpha) = {
    val nowBucket = System.currentTimeMillis / AccessStatsConstants.bucketSize

    var mean = 0.0
    var numBuckets = 0
    var lastBucket:Long = nowBucket

    buckets.foreach(b => {
      if (b.bucket < nowBucket && numBuckets < 6) {
        mean += b.num.toDouble
        numBuckets += (lastBucket - b.bucket).toInt
        if (numBuckets > 6) numBuckets = 6
        lastBucket = b.bucket
      }
    })

    if (numBuckets > 0) mean = mean / numBuckets.toDouble
    mean
  }

}

object Likelihood {
  private def factorial(x: Long) : Long = {
    def factorialWithAccumulator(accumulator: Long, number: Long) : Long = {
      if (number <= 1) {
        return accumulator
      } else {
        factorialWithAccumulator(accumulator * number, number - 1)
      }
    }
    factorialWithAccumulator(1, x)
  }

  private def poisson(k: Int, lambda: Double) = {
    scala.math.pow(lambda, k) * scala.math.pow(scala.math.E, -lambda) / factorial(k)
  }

  // http://www.mathpages.com/home/kmath580/kmath580.htm

  private def noOverlap(width: Double, n: Int) = {
    scala.math.pow((1.0 - (n - 1.0) * width / AccessStatsConstants.bucketSize), n)
  }

  // avg is # accesses in window.
  // classic paxos.
  def rowSuccess(md: MDCCMetadata) = {
    val avg = md.getAvgAccessRate()
    val masterDC = LatencySampling.hostToDC(md.currentVersion.server.host)
    val localDC = LatencySampling.localDC

    val masterDelay = LatencySampling.getMasterDelayFactor(masterDC)
    val clientDelay = LatencySampling.getClientDelayFactor(localDC)

    // master quorum round-trip + visibility + propose to master
    val masterQuorum = LatencySampling.getQuorumRoundTrip(masterDC, 3) * (1.0 + masterDelay)
    // TODO: 3 recs?
    val visibility = LatencySampling.getAvgDCToVisibility(masterDC, localDC, 3)
    val localRead = LatencySampling.getRoundTrip(localDC, localDC) * (1.0 + clientDelay) / 2.0
    val localPropose = LatencySampling.getRoundTrip(localDC, masterDC) * (1.0 + clientDelay) / 2.0

    val duration = masterQuorum + visibility + localPropose

    val pr = poisson(0, duration * avg / AccessStatsConstants.bucketSize)

    println("pr: " + pr + " avg: " + avg + " masterDC: " + masterDC + " duration: " + duration + " = (" + masterQuorum + "," + visibility + "," + localPropose + ")" + " stats: " + md.statistics)

    pr
  }

  def rowDuration(md: MDCCMetadata) = {
    val masterDC = LatencySampling.hostToDC(md.currentVersion.server.host)
    val localDC = LatencySampling.localDC

    val localPropose = LatencySampling.getRoundTrip(localDC, masterDC)
    val masterQuorum = LatencySampling.getQuorumRoundTrip(masterDC, 3)

    val duration = localPropose + masterQuorum

    duration
  }

  // assumed classic.
  def rowLearned(md: MDCCMetadata, predictedTime: Double, actualTime: Long) = {
    val masterDC = LatencySampling.hostToDC(md.currentVersion.server.host)

    val diff = actualTime.toDouble - predictedTime
    val diffPercent = diff / predictedTime

    LatencySampling.addVariance(masterDC, diffPercent)

//    println("rowLearnedTime: masterDC: " + masterDC + " timeDiff: " + diff + " timeDiff%: " + diffPercent)
  }


}

class ZipfGenerator(val size: Int, val skew: Double) {
  var bottom: Double = 0

  val rand = new scala.util.Random

  //calculate the generalized harmonic number of order 'size' of 'skew'
  //http://en.wikipedia.org/wiki/Harmonic_number

  (1 to size).foreach(i => {
    bottom += (1.0 / math.pow(i, skew))
  })

  // Method that returns a rank id between 0 and this.size (exclusive).

  def nextInt() = {
    var rank: Int = -1
    var frequency: Double = 0.0
    var dice: Double = 0.0

    while (dice >= frequency) {
      rank = rand.nextInt(size)
      frequency = getProbability(rank + 1)
      dice = rand.nextDouble()
    }
    rank
  }

  def getProbability(rank: Int): Double = {
    if (rank == 0) {
      0
    } else {
      (1.0 / math.pow(rank, skew)) / bottom
    }
  }
}
