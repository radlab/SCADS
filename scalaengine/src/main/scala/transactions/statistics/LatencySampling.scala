package edu.berkeley.cs.scads.storage
package transactions

import edu.berkeley.cs.scads.comm._
import _root_.edu.berkeley.cs.avro.marker.AvroRecord
import scala.math.{min, max}
import collection.mutable.{ArrayBuffer, ArraySeq, HashMap, Queue}

object LatencySampling {
  var servers: List[StorageService] = List()
  var dcs: List[StorageService] = List()
  var samplerThread: Thread = null
  var fastSampler: FastSampler = new FastSampler

  val localData: HashMap[String, LocalLatencyData] = HashMap()
  // src_dc -> (dst_dc -> info list)
  val globalData: HashMap[String, HashMap[String, LatencyInfoList]] = HashMap()

  val localDelayData: HashMap[String, LatencyDelayInfo] = HashMap()
  // src_dc -> (dst_dc -> delay info)
  val globalDelayData: HashMap[String, HashMap[String, LatencyDelayInfo]] = HashMap()
  val globalMasterDelay: HashMap[String, LatencyDelayInfo] = HashMap()
  val globalClientDelay: HashMap[String, LatencyDelayInfo] = HashMap()
  var defaultDelay = LatencyDelayInfo(0, 0, 0.0)

  val localDC = hostToDC(java.net.InetAddress.getLocalHost.getCanonicalHostName())

  // buckets boundaries for latencies.
  val buckets = List(0, 100, 200, 300, 400, 500)

  // probabilities that dc X is the slowest of N record transactions.
  // assuming 5 dcs. assuming at most 10 records.
  val txDelayProbs = (1 to 10).toList.map(numRecords => {
    var probList = new scala.collection.mutable.ListBuffer[Double]
    var runningProb = 0.0
    (1 to 5).foreach(dc => {
      val base = if (dc == 5) {
        1.0
      } else {
        scala.math.pow((dc.toDouble / 5.0), numRecords)
      }
      val thisProb = base - runningProb
      runningProb += thisProb
      probList.append(thisProb)
    })
    probList.toList
  })

  class FastSampler extends Runnable {
    var shouldStop = false
    var waitTime = 5

    def signalStop() {
      shouldStop = true
    }

    def setWait(w: Int) {
      waitTime = w
    }

    def run() {
      while(!shouldStop) {
        val output = localData.mapValues(_.toLatencyInfoList).toMap
        val delayOutput = localDelayData.toMap
        val outputMsg = LatencyPing(output, delayOutput)

        // client -> (dc -> info list)
        var lastResult: scala.collection.Map[String, scala.collection.Map[String, LatencyInfoList]] = null

        // client -> (dc -> delay info)
        var lastDelayResult: scala.collection.Map[String, scala.collection.Map[String, LatencyDelayInfo]] = null

        dcs.foreach(s => {
          val dc = hostToDC(s.host)

          val start = System.nanoTime / 1000000
          s !? outputMsg match {
            case LatencyPingResponse(m, d) =>
              val end = System.nanoTime / 1000000
              val latency = end - start
              if (localData.contains(dc)) {
                // update existing data for this dc.
                localData.get(dc).get.add(latency)
              } else {
                // insert new data for dc.
                val l = new LocalLatencyData
                l.add(latency)
                localData.put(dc, l)
              }

              lastResult = m
              lastDelayResult = d
            case _ =>
          }
        })

        // aggregate global data.
        if (lastResult != null) {
          val groups = lastResult.groupBy(x => hostToDC(x._1))
          groups.map(x => {
            // group val
            val g = x._1
            // list of values (client -> (dc -> info list))
            val l = x._2

            val m = new HashMap[String, LatencyInfoList]
            l.foreach(c => {
              c._2.foreach(i => {
                val dc1 = i._1
                val info = i._2
                if (m.contains(dc1)) {
                  m.get(dc1).get.merge(info)
                } else {
                  m.put(dc1, info)
                }
              })
            })

            globalData.put(g, m)
          })
        }

//        println("global: " + globalData)

        // aggregate global delay data.
        defaultDelay = LatencyDelayInfo(0, 0, 0.0)
        if (lastDelayResult != null) {
          val groups = lastDelayResult.groupBy(x => hostToDC(x._1))
          groups.map(x => {
            // group val
            val g = x._1
            // list of values (client -> (dc -> delay info))
            val l = x._2

            val m = new HashMap[String, LatencyDelayInfo]
            l.foreach(c => {
              c._2.foreach(i => {
                // master dc
                val dc1 = i._1
                val info = i._2
                if (m.contains(dc1)) {
                  m.get(dc1).get.merge(info)
                } else {
                  m.put(dc1, info)
                }
                defaultDelay.merge(info)
                // group by master
                if (globalMasterDelay.contains(dc1)) {
                  globalMasterDelay.get(dc1).get.merge(info)
                } else {
                  globalMasterDelay.put(dc1, LatencyDelayInfo(info.totalCount, info.delayCount, info.sum))
                }
                // group by client
                if (globalClientDelay.contains(g)) {
                  globalClientDelay.get(g).get.merge(info)
                } else {
                  globalClientDelay.put(g, LatencyDelayInfo(info.totalCount, info.delayCount, info.sum))
                }
              })
            })

            globalDelayData.put(g, m)
          })
        }

//        println("globalDelay: " + globalDelayData)

        Thread.sleep(waitTime)
      }
    }
  }

  def startSampling(s: Seq[StorageService]) = {
    this.synchronized {
      if (servers.length == 0) {
        servers = s.toList
        // (dc, list(services))
        val groups = servers.groupBy(x => hostToDC(x.host))
        dcs = groups.values.toList.map(l => l.sortBy(x => x.toString).head)

        dcs.foreach(s => {
          val dc = hostToDC(s.host)
          if (!localData.contains(dc)) localData.put(dc, new LocalLatencyData)
        })

        samplerThread = new Thread(fastSampler)
        samplerThread.start()
      }
    }
  }

  def slowSampling(w: Int) = {
    this.synchronized {
      if (samplerThread != null) {
        fastSampler.setWait(w)
      }
    }
  }

  def stopSampling() = {
    this.synchronized {
      if (samplerThread != null) {
        fastSampler.signalStop()
        samplerThread.join()
        samplerThread = null
      }
    }
  }

  def getMasterDelayFactor(srcDC: String): Double = {
    globalMasterDelay.get(srcDC) match {
      case None => defaultDelay.getDelayFactor()
      case Some(d) => d.getDelayFactor()
    }
  }

  def getClientDelayFactor(srcDC: String): Double = {
    globalClientDelay.get(srcDC) match {
      case None => defaultDelay.getDelayFactor()
      case Some(d) => d.getDelayFactor()
    }
  }

  def getDelayFactor(srcDC: String, dstDC: String): Double = {
    globalDelayData.get(srcDC) match {
      case None => defaultDelay.getDelayFactor()
      case Some(h) => h.get(dstDC) match {
        case None => defaultDelay.getDelayFactor()
        case Some(d) => d.getDelayFactor()
      }
    }
  }

  // round trip time between source DC a quorum.
  def getQuorumRoundTrip(srcDC: String, qSize: Int): Double = {
    val delayFactor = getMasterDelayFactor(srcDC)
    globalData.get(srcDC) match {
      case None => 143.74
      case Some(h) => h.values.toList.sortBy(x => x.mean).take(qSize).last.mean
    }
  }

  // round trip time between two DCs.
  def getRoundTrip(srcDC: String, dstDC: String): Double = {
    globalData.get(srcDC) match {
      case None => 143.74
      case Some(h) => h.get(dstDC) match {
        case None => 143.74
        case Some(li) => li.mean
      }
    }
  }

/*
  // worked well for single record transactions.
  // master to client dc, client dc to current dc, for all possible client dcs
  def getAvgDCToVisibility(dc1: String, dc2: String, numRecords: Int = 1): Double = {
    val times = globalData.keys.toList.map(d => {
      val d1 = getDelayFactor(d, dc1)
      val d2 = getClientDelayFactor(d)
      (getRoundTrip(dc1, d) * (1.0 + d1) + getRoundTrip(d, dc2) * (1.0 + d2)) / 2.0
    })
//    times.sum.toDouble / times.length
    // try max, max works much better than avg.
    times.max.toDouble
  }
*/

  // for multi-record transactions.
  // master to client dc, client dc to current dc, for all possible client dcs
  def getAvgDCToVisibility(masterDC1: String, currentDC2: String, numRecords: Int = 1): Double = {
    val allDCs = globalData.keys.toList
    val times = allDCs.map(clientDC => {
      val latencyToDC1 = getRoundTrip(clientDC, masterDC1) * (1.0 + getDelayFactor(clientDC, masterDC1))

      // given the master and client and number of records in the hypothetical
      // tx, how long did it take?
      // average out between all possible masters for the hypothetical records.
      // however, one of the records has to be in the masterDC1.
      val remainingRecords = numRecords - 1

      // avg latency for this client to execute the transaction.
      // does not include the visibility message.
      val avgLatency = if (remainingRecords == 0) {
        latencyToDC1
      } else {
        val id = scala.math.min(remainingRecords, txDelayProbs.length - 1)
        val probs = txDelayProbs(id)

        val allLatencies = allDCs.map(dcX => {
          val d1 = getDelayFactor(clientDC, dcX)
          getRoundTrip(clientDC, dcX) * (1.0 + d1)
        }).sortWith(_ < _)
        // since the masterDC1 is set, all latencies have to be as least that.
        val scaledLatencies = allLatencies.map(scala.math.max(latencyToDC1, _))

        // weight the latencies with probability.
        val weighted = scaledLatencies.zip(probs).map { case(l, p) => l * p }
        weighted.sum
      }

      // subtract out the propose message to the master.
      val beforeVisibility = avgLatency - latencyToDC1 / 2.0

      // add visibility message.
      val d2 = getClientDelayFactor(clientDC)
      beforeVisibility + (getRoundTrip(clientDC, currentDC2) * (1.0 + d2)) / 2.0
    })
    times.sum.toDouble / times.length
  }

  def hostToDC(host: String) = host.split("\\.")(1)


  def addVariance(masterDC: String, diffPercent: Double) = {
    this.synchronized {
      if (localDelayData.contains(masterDC)) {
        localDelayData.get(masterDC).get.add(diffPercent)
      } else {
        val l = new LatencyDelayInfo(0, 0, 0.0)
        l.add(diffPercent)
        localDelayData.put(masterDC, l)
      }
    }
  }

}

class LocalLatencyData {
  // check out if needed: http://www.johndcook.com/standard_deviation.html

  val numHistory = 50
  val data = new Queue[Long]
  var sum: Long = 0

  val bucketSums = ArrayBuffer(LatencySampling.buckets.map(_ => 0L) : _*)
  val bucketCounts = ArrayBuffer(LatencySampling.buckets.map(_ => 0) : _*)

  def add(l: Long) = {
    data.enqueue(l)

    val i = latencyToBucket(l)
    bucketSums(i) += l
    bucketCounts(i) += 1
    if (data.length > numHistory) {
      val old = data.dequeue()
      val i2 = latencyToBucket(old)
      bucketSums(i2) -= old
      bucketCounts(i2) -= 1
    }
  }

  def toLatencyInfoList = LatencyInfoList(bucketSums.zip(bucketCounts).map(x => LatencyInfo(x._2, x._1)).toList)

  def latencyToBucket(l: Long) = {
    var bucket = -1
    LatencySampling.buckets.zipWithIndex.reverse.foreach(x => {
      if (bucket < 0) {
        if (l >= x._1) bucket = x._2
      }
    })
    if (bucket < 0) bucket = 0
    bucket
  }
}

