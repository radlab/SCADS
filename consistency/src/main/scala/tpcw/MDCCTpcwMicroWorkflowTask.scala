package edu.berkeley.cs
package scads
package consistency
package tpcw

import edu.berkeley.cs.scads.piql.tpcw._
import edu.berkeley.cs.scads.piql.exec._
import edu.berkeley.cs.scads.storage.transactions._

import comm._
import perf._
import storage._
import avro.runtime._
import avro.marker._

import deploylib._
import deploylib.mesos._
import deploylib.ec2._

import scala.util.Random
import scala.collection.{ mutable => mu }

import scala.collection.JavaConversions._

case class MDCCMicroBenchmarkResult(var clientConfig: MDCCTpcwMicroWorkflowTask,
                                    var loaderConfig: MDCCTpcwMicroLoaderTask,
                                    var clusterAddress: String,
                                    var clientId: Int,
                                    var iteration: Int,
                                    var threadId: Int) extends AvroPair {
  var startTime: String = _
  var totalElaspedTime: Long = _ /* in ms */
  var times: Map[String, Histogram] = null
  var failures: Int = _
}

// additionalSettings:
//    "hotspot": int, percent of hotspot size, 90 is uniform
//    "localMaster": int, pecent of local masters, -1 is unused
//    "useSpecCommit": int 0 for false, 1 for true
case class MDCCTpcwMicroWorkflowTask(var numClients: Int,
                                     var executorClass: String,
                                     var numThreads: Int = 10,
                                     var iterations: Int = 3,
                                     var runLengthMin: Int = 5,
                                     var useLogical: Boolean = false,
                                     var startTime: String,
                                     var clusterId: Int = 0,
                                     var numClusters: Int = 1,
                                     var programmingModelTest: Boolean = false,
                                     var additionalSettings: Map[String, String] = Map(),
                                     var note: String) extends AvroRecord with ReplicatedExperimentTask {

  var experimentAddress: String = _
  var clusterAddress: String = _
  var resultClusterAddress: String = _

  def clearItem(itemId: String) = {
    val item = MicroItem(itemId)
    item.I_TITLE = ""
    item.I_A_ID = ""
    item.I_PUB_DATE = 0
    item.I_PUBLISHER = ""
    item.I_SUBJECT = ""
    item.I_DESC = ""
    item.I_RELATED1 = 0
    item.I_RELATED2 = 0
    item.I_RELATED3 = 0
    item.I_RELATED4 = 0
    item.I_RELATED5 = 0
    item.I_THUMBNAIL = ""
    item.I_IMAGE = ""
    item.I_SRP = 0
    item.I_COST = 0
    item.I_AVAIL = 0
    item.I_STOCK = 0
    item.ISBN = ""
    item.I_PAGE = 0
    item.I_BACKING = ""
    item.I_DIMENSION = ""
    item
  }

  def getMetadataRegion(ns: PairNamespace[MicroItem] with PairTransactions[MicroItem], item: MicroItem): String = {
    val hash = java.util.Arrays.hashCode(ns.keyToBytes(item.key))
    val rand = new scala.util.Random(hash)

    val randomRegion = if (rand.nextInt(100) < 20) {
      "us-west-1"
    } else {
      val l = List("compute-1", "eu-west-1", "ap-northeast-1", "ap-southeast-1")
      l(rand.nextInt(l.size))
    }
    randomRegion
  }

  def run(): Unit = {

    val numGlobalClients = numClients * numClusters

    val clientId = coordination.registerAndAwait("clientStart", numGlobalClients, timeout=60*60*1000)

    logger.info("Waiting for cluster to be ready")
    val clusterConfig = clusterRoot.awaitChild("clusterReady")
    val loaderConfig = classOf[MDCCTpcwMicroLoaderTask].newInstance.parse(clusterConfig.data)

    val results = resultCluster.getNamespace[MDCCMicroBenchmarkResult]("MicroBenchmarkResults")
    val executor = Class.forName(executorClass).newInstance.asInstanceOf[QueryExecutor]

    val loader = new TpcwLoader(
      numEBs = loaderConfig.numEBs,
      numItems = loaderConfig.numItems)

    val random = new Random
    // out of stock items.
    val oosItems = new mu.BitSet(loaderConfig.numItems)

    // List(USWest1, USEast1, EUWest1, APNortheast1, APSoutheast1)
    val ownRegion = clusterId match {
      case 0 => "us-west-1"
      case 1 => "compute-1"
      case 2 => "eu-west-1"
      case 3 => "ap-northeast-1"
      case 4 => "ap-southeast-1"
      case _ => "wrong_cluster_id"
    }

    // For classic master locality test.
    val testMicroItems = cluster.getNamespace[MicroItem]("microItems", loaderConfig.txProtocol)
    val localItemIds = new scala.collection.mutable.ArrayBuffer[Int]()
    val remoteItemIds = new scala.collection.mutable.ArrayBuffer[Int]()
    (1 to loaderConfig.numItems).foreach(id => {
      if (getMetadataRegion(testMicroItems, MicroItem(loader.toItem(id))) == ownRegion) {
        localItemIds.append(id)
      } else {
        remoteItemIds.append(id)
      }
    })
    logger.info("localItemIds: %d", localItemIds.size)
    logger.info("remoteItemIds: %d", remoteItemIds.size)

    // TODO: read this from the map, not the note.
    val localMaster = note.indexOf("localMaster_") match {
      case -1 =>
        -1
      case i =>
        note.substring(i + 12).toInt
    }
    logger.info("localMaster: %d", localMaster)

    // TODO: read this from the map, not the note.
    // requested hotspot percent
    val hotspot_percent = note.indexOf("hot_") match {
      case -1 =>
        90
      case i =>
        val rest = note.substring(i + 4)
        rest.indexOf("_") match {
          case -1 =>
            note.substring(i + 4).toInt
          case j =>
            note.substring(i + 4, i + 4 + j).toInt
        }
    }

    val hotspot_cutoff = if (hotspot_percent < 0) {
      // hotspot_percent < 0 means, just use that many items, not a percent.
      -hotspot_percent
    } else {
      loaderConfig.numItems * hotspot_percent / 100
    }

    logger.info("Hotspot info: percent: %d, id cutoff: %d, numItems: %d", hotspot_percent, hotspot_cutoff, loaderConfig.numItems)

    // using speculative commit.  only when using programming model test.
    val useSpecCommit = if (programmingModelTest && additionalSettings.contains("useSpecCommit")) {
      additionalSettings.get("useSpecCommit").get.toInt == 1
    } else {
      false
    }

    // number of rows in a tx. (1, 1) means exactly 1 record
    val txMinSize = additionalSettings.getOrElse("txMinSize", "1").get.toInt
    val txSizeRange = additionalSettings.getOrElse("txSizeRange", "1").get.toInt

    // use zipfian distribution
    val useZipf = additionalSettings.getOrElse("useZipf", "0").get.toInt == 1
    val randomZipf = new ZipfGenerator(loaderConfig.numItems, 0.1)

    logger.info("programmingModelTest: %s", programmingModelTest)
    logger.info("useSpecCommit: %s", useSpecCommit)
    logger.info("txSizes: (%d - %d)", txMinSize, txMinSize + txSizeRange - 1)
    logger.info("useZipf: %s", useZipf)


    // returns the (name, id) of a random item.
    def randomItem(forceHotspot: Boolean = false, avoidHotspot: Boolean = false) = {
      var id = 0
      do {
        if ((hotspot_percent == 90) && !forceHotspot && !avoidHotspot) {
          if (useZipf) {
            // use zipf distribution.
            id = randomZipf.nextInt() + 1
          } else {
            // Uniformly random item.
            id = random.nextInt(loaderConfig.numItems) + 1
          }
        } else {
          // use hotspot access pattern
          if (!avoidHotspot && ((random.nextInt(100) < 90) || forceHotspot)) {
            // pick a random item from the hotspot.
            id = random.nextInt(hotspot_cutoff) + 1
          } else {
            // pick a random item from the cold spot.
            id = random.nextInt(loaderConfig.numItems - hotspot_cutoff) + 1 + hotspot_cutoff
          }
        }
      } while (oosItems.contains(id))
      val ret = (loader.toItem(id), id)
      ret
    }

    def randomLocalItem = {
      var id = 0
      val pickLocal = random.nextInt(100) < localMaster
      val itemIds = if (pickLocal) localItemIds else remoteItemIds
      do {
        id = itemIds.get(random.nextInt(itemIds.size))
      } while (oosItems.contains(id))
      val ret = (loader.toItem(id), id)
      ret
    }

    // buyList is a list of ((itemName, itemId), # to buy)
    def getBuyList() = {
//      val itemIds = (1 to random.nextInt(10) + 1).map(_ => randomItem()).toSet.toSeq

      val numRecs = txMinSize + random.nextInt(txSizeRange)




      // usually use this for micro test.
//      val itemIds = (1 to numRecs).map(_ => randomItem()).toSet.toSeq

      // localMaster test
//      val itemIds = (1 to numRecs).map(_ => randomLocalItem).toSet.toSeq



      // programming model test. use 1 rec?
//      val itemIds = (1 to numRecs).map(_ => randomItem()).toSet.toSeq

      val itemIds = if (hotspot_percent == 90) {
        // just normal item picking
        (1 to numRecs).map(_ => randomItem()).toSet.toSeq
      } else {
        if (random.nextInt(100) < 90) {
          // touch the hotspot.  basically, make 90% of TX hit hotspot,
          // not individual records.
          (1 to numRecs).map(i => {
            if (i == 1) {
              // first one is always in hotspot.
              randomItem(true)
            } else {
              // other ones may or may not be in the hotspot.
              randomItem()
            }
          })
        } else {
          // do not touch hotspot.
          (1 to numRecs).map(_ => randomItem(false, true)).toSet.toSeq
        }
      }




//      val itemIds = (1 to random.nextInt(3) + 1).map(_ => randomItem()).toSet.toSeq
      itemIds.map(i => (i, random.nextInt(3) + 1))
    }

    // buyList is a list of ((itemName, itemId), # to buy)
    def getSingularBuyList() = {
      List(((loader.toItem(1), 1), 1))
    }

    var stopTest = false

    def buyAction(ns: PairNamespace[MicroItem] with PairTransactions[MicroItem], duringWarmup: Boolean, startTime: Long, specResults: java.util.concurrent.ConcurrentLinkedQueue[(String, Long)]): (TxStatus, String, Boolean) = {
      val buyList = getBuyList()
//      val buyList = getSingularBuyList()  // inconsistency test

      var rowsProbList: List[RowLikelihood] = null
      var txLikelihood = 0.0

      var aName = if (useLogical) "buyAction-Write-logical" else "buyAction-Write-physical"

      val startT = System.nanoTime / 1000000
      var usedHotspot = false
      var commit = true
      var speculated = false
      val tx = new Tx(10000, ReadLocal()) ({
        val i1 = buyList.map(i => {
          // ((future, item id), buy amt)
          ((ns.asyncGetRecord(MicroItem(i._1._1)), i._1._2), i._2)
        })

        val i2 = i1.map(i => {
          val x = i._1._1().get
          if (x.I_STOCK == 0) {
            oosItems.add(i._1._2)
            println("oos: " + oosItems.size)
          } else if (hotspot_percent < 90 && i._1._2 <= hotspot_cutoff) {
            usedHotspot = true
          }
          // (MicroItem, buy amt)
          (x, scala.math.min(x.I_STOCK, i._2))
        })
        if (!i2.exists(i => i._2 != 0)) {
          // No items in cart are in stock.
          // Do not write items.
          commit = false
//          stopTest = true  // inconsistency test
        } else {
          i2.foreach(i => {
            if (!useLogical) {
              val item = i._1
              item.I_STOCK -= i._2
              ns.put(item)
            } else {
              val item = clearItem(i._1.I_ID)
              item.I_STOCK = -i._2
              ns.putLogical(item)
            }
          })
        }
      }).Probabilistic((rowsProb: Seq[RowLikelihood], txProb: Double) => {
        rowsProbList = rowsProb.toList
        txLikelihood = txProb
        if (useSpecCommit && txProb >= 0.95) {
          speculated = true
          true
        } else {
          false
        }
      }).Finally((status: TxStatus, timedOut: Boolean, endSpecTime: Long) => {
        if (useSpecCommit && !duringWarmup && endSpecTime > 0) {
          val actionName = if (status == COMMITTED) {
            aName + "-SPEC-COMMIT"
          } else {
            aName + "-SPEC-ABORT"
          }
          val elapsedTime = endSpecTime - startTime
          specResults.add((actionName, elapsedTime))
        }
      })

      val txStatus = tx.Execute() match {
        case COMMITTED => if (!commit) ABORTED else COMMITTED
        case x => x
      }
      val endT = System.nanoTime / 1000000

      if (!duringWarmup) {
        println(txStatus + " likelihood: " + txLikelihood + " rowsProb: " + rowsProbList + " real_duration: " + (endT - startT))
      }

      if (usedHotspot) aName = aName + "-hotspot"
      (txStatus, aName, speculated)
    }



    /****************************************************************/
    /****************************************************************/
    /****************************************************************/

    val warmupTime = 2 * 60 * 1000
//    val warmupTime = -1  // inconsistency test

    // Sync up before collecting latency stats
    coordination.registerAndAwait("beforelatencystats", numGlobalClients, timeout=60*60*1000)

    val samplingServers = (0 until loaderConfig.numClusters).map(c => {
      cluster.getAvailableServers("cluster-" + c).sortBy(x => x.toString).head
    })
    LatencySampling.startSampling(samplingServers)
    Thread.sleep(45*1000)
    LatencySampling.slowSampling(1000)

    // Sync up with other clients.
    coordination.registerAndAwait("iteration1start", numGlobalClients, timeout=60*60*1000)

    logger.info("starting experiment at: " + startTime)

    for(iteration <- (1 to iterations)) {
      logger.info("Begining iteration %d", iteration)
      val resultList = (1 to numThreads).pmap(threadId => {
        val microItems = cluster.getNamespace[MicroItem]("microItems", loaderConfig.txProtocol)
        def getTime = System.nanoTime / 1000000
        val histograms = new mu.HashMap[String, Histogram]
        val runTime = runLengthMin * 60 * 1000L
        val iterationStartTime = getTime
        var endTime = iterationStartTime
        var failures = 0
        // (name, elapsed time)
        val specResults = new java.util.concurrent.ConcurrentLinkedQueue[(String, Long)]()

        while(endTime - iterationStartTime < (runTime + warmupTime)) {
//        while(!stopTest) {  // inconsistency test

          val startTime = getTime
          try {
            val duringWarmup = (endTime - iterationStartTime) <= warmupTime
            val (actionStatus, aName, speculated) = buyAction(microItems, duringWarmup, startTime, specResults)
            endTime = getTime
            if (!duringWarmup && !speculated) {
              val actionName = if (actionStatus == COMMITTED) {
                aName + "-COMMIT"
              } else if (actionStatus == REJECTED) {
                aName + "-REJECT"
              } else {
                aName + "-ABORT"
              }
              val elapsedTime = endTime - startTime
              if (histograms.isDefinedAt(actionName)) {
                // Histogram for this action already exists.
                histograms.get(actionName).get += elapsedTime
              } else {
                // Create a histogram for this action.
                val newHist = Histogram(1, 10000)
                newHist += elapsedTime
                histograms.put(actionName, newHist)
              }
            }
          } catch {
            case e => {
              logger.warning(e, "Execepting generating page")
              failures += 1
            }
          }
        }  // while loop for time

        // add in all the spec results
        specResults.iterator.foreach(x => {
          val actionName = x._1
          val elapsedTime = x._2
          println("specResult: " + actionName + " : " + elapsedTime)
          if (histograms.isDefinedAt(actionName)) {
            // Histogram for this action already exists.
            histograms.get(actionName).get += elapsedTime
          } else {
            // Create a histogram for this action.
            val newHist = Histogram(1, 10000)
            newHist += elapsedTime
            histograms.put(actionName, newHist)
          }
        })

        val res = MDCCMicroBenchmarkResult(this, loaderConfig, clusterRoot.canonicalAddress, clientId, iteration, threadId)
        res.startTime = startTime
        res.totalElaspedTime = endTime - iterationStartTime - warmupTime
        res.times = histograms.toMap
        res.failures = failures

        res
      })  // threads pmap

      logger.info("******** aggregate results.")

      // Aggregate all the threads into 1 result.
      var res = resultList.head
      resultList.tail.foreach(r => {
        val histograms = new mu.HashMap[String, Histogram]
        histograms ++= res.times
        r.times.foreach(x => {
          val (k, h) = x
          if (histograms.isDefinedAt(k)) {
            histograms.put(k, histograms.get(k).get + h)
          } else {
            histograms.put(k, h)
          }
        })

        res.times = histograms.toMap
        res.failures += r.failures
      })

      res.times.foreach(x => {
        val (k, h) = x
        logger.info("%s stats count: %d 50th: %dms, 90th: %dms, 99th: %dms, avg: %fms, stddev: %fms",
                    k, h.totalRequests, h.quantile(0.50), h.quantile(0.90), h.quantile(0.99), h.average, h.stddev)
      })

      logger.info("******** writing out aggregated results.")
      results ++= List(res)

      coordination.registerAndAwait("iteration" + iteration, numGlobalClients)
    }

    if(clientId == 0) {
      ExperimentNotification.completions.publish("TPCW MDCC Complete", this.toJson)
      cluster.shutdown
    }
  }
}
