package edu.berkeley.cs
package scads
package consistency

import deploylib._
import deploylib.ec2._
import deploylib.mesos._
import avro.marker._
import comm._
import config._
import storage._

import net.lag.logging.Logger

import edu.berkeley.cs.scads.storage.transactions._
import edu.berkeley.cs.scads.storage.transactions.FieldAnnotations._
import edu.berkeley.cs.scads.perf._
import edu.berkeley.cs.scads.piql.tpcw._

import scala.actors.Future
import scala.actors.Futures._

import tpcw._

object MicroBenchmark extends ExperimentBase {
  val logger = Logger()
  val clusters = getClusters()

  def getClusters() = {
    val regions = List(USWest1, USEast1, EUWest1, APNortheast1, APSoutheast1)
    regions.map(new mesos.Cluster(_))
  }

  def restartClusters(c: Seq[deploylib.mesos.Cluster] = clusters) = {
    c.map(v => future {println("restarting " + v); v.restart; println("done " + v)}).map(_())
  }

  def updateJars(c: Seq[deploylib.mesos.Cluster] = clusters) = {
    c.map(v => future {println("updating " + v); v.slaves.pforeach(_.pushJars(MesosCluster.jarFiles)); println("done " + v)}).map(_())
  }

  def setupClusters(sizes: Seq[Int], c: Seq[deploylib.mesos.Cluster] = clusters) = {
    if (sizes.size != c.size) {
      println("sizes has to be the same length has c. " + sizes.size + " != " + c.size)
    } else {
      c.zip(sizes).map(v => future {println("setup " + v._1); v._1.setup(v._2); println("done " + v)}).map(_())
    }
  }

  private var actionHistograms: Map[String, scala.collection.mutable.HashMap[String, Histogram]] = null

  private def addHist(aggHist: scala.collection.mutable.HashMap[String, Histogram], key: String, hist: Histogram) = {
    if (aggHist.isDefinedAt(key)) {
      aggHist.put(key, aggHist.get(key).get + hist)
    } else {
      aggHist.put(key, hist)
    }
  }

  def expName(res: tpcw.MDCCMicroBenchmarkResult) = {
    val additionalSettings = res.clientConfig.microSettings.toMap
    val spec = if (res.clientConfig.programmingModelTest && (additionalSettings.getOrElse("useSpecCommit", "0") == "1")) {
      "_spec"
    } else {
      ""
    }

    val notes = res.clientConfig.note + spec
    val logical = if (res.clientConfig.useLogical) "logical" else "physical"
    res.startTime + " (" + res.clientConfig.numClusters + "DC." + res.clientConfig.numClients + "." + res.clientConfig.numThreads + "=" + (res.clientConfig.numClusters * res.clientConfig.numClients * res.clientConfig.numThreads) + ")(" + res.loaderConfig.numItems + ")-" + res.loaderConfig.txProtocol.toString.replace("NSTxProtocol", "").replace("()", "") + " " + logical + " " + notes
  }

  // Only gets histograms with something greater than 'name'
  def getActionHistograms(name: String = "2012-06-26 09:59:16.992") = {
    val resultNS = resultCluster.getNamespace[MDCCMicroBenchmarkResult]("MicroBenchmarkResults")
    val histograms = resultNS.iterateOverRange(None, None)
    .filter(r => expName(r) > name)
    .toSeq
    .groupBy(r => (r.startTime, r.loaderConfig.txProtocol, r.clientConfig.useLogical, r.clientConfig.note))
    .map {
      case( (startTime, txProtocol, logical, note), results) => {
        val aggHist = new scala.collection.mutable.HashMap[String, Histogram]
        results.map(_.times).foreach(t => {
          t.foreach(x => {
            addHist(aggHist, x._1, x._2)

            // Group Bys...
            val txType = if (x._1.contains("Read")) "READ" else "WRITE"
            val txCommit = if (x._1.contains("COMMIT")) "COMMIT" else "ABORT"
            addHist(aggHist, txType, x._2)
            addHist(aggHist, txType + "-" + txCommit, x._2)
            addHist(aggHist, "TOTAL", x._2)
            addHist(aggHist, "TOTAL" + "-" + txCommit, x._2)
          })
        })
        println(expName(results.head) + "      " + results.size)
        (expName(results.head), aggHist)
      }
    }.toList.toMap

    println("sorted keys: ")
    histograms.keys.toList.sortWith(_.compare(_) < 0).foreach(println _)

    actionHistograms = histograms
    histograms
  }

  def deleteHistograms(name: String) = {
    val resultNS = resultCluster.getNamespace[MDCCMicroBenchmarkResult]("MicroBenchmarkResults")
    val histList = new collection.mutable.ArrayBuffer[MDCCMicroBenchmarkResult]()
    val histograms = resultNS.iterateOverRange(None, None).foreach(r => {
      val s = expName(r)
      if (name == s) {
        histList.append(r)
      }
    })
    histList.foreach(resultNS.delete(_))
    println("deleted " + histList.size + " records")
  }

  def writeHistogramCDFMap(name: String, write: Boolean = true) = {
    val mh = actionHistograms(name)
    try {
      val dirs = ("micro_data/" + name + "/").replaceAll(" ", "_")
      if (write) (new java.io.File(dirs)).mkdirs

      val readmeFilename = dirs + ("README.txt").replaceAll(" ", "_")
      val readmeOut =
        if (write)
          new java.io.BufferedWriter(new java.io.FileWriter(readmeFilename))
        else
          null

      mh.foreach(t => {
        val n = t._1
        val h = t._2
        val totalRequests = h.totalRequests
        val filename = dirs + ("micro_" + n + ".csv").replaceAll(" ", "_")

        var total:Long = 0
        val out =
          if (write)
            new java.io.BufferedWriter(new java.io.FileWriter(filename))
          else
            null
        ((0 until h.buckets.length).map(_ * h.bucketSize) zip h.buckets).foreach(x => {
          total = total + x._2
          if (write) out.write(" " + x._1 + ", " + total * 100.0 / totalRequests.toFloat + "\n")
        })
        if (write) out.close()

        if (write) {
          val rawFilename = dirs + ("RAW_micro_" + n + ".csv").replaceAll(" ", "_")
          val rawOut = new java.io.BufferedWriter(new java.io.FileWriter(rawFilename))
          ((0 until h.buckets.length).map(_ * h.bucketSize) zip h.buckets).foreach(x => {
            rawOut.write(" " + x._1 + ", " + x._2 + "\n")
          })
          rawOut.close()
        }

        val extremes = h.boxplotExtremes(false)
        val extremesNoOutliers = h.boxplotExtremes(true)

        val summary = n + ": requests: " + totalRequests + " 50%: " + h.quantile(.5) + " 90%: " + h.quantile(.9) + " 95%: " + h.quantile(.95) + " 99%: " + h.quantile(.99) + " avg: " + h.average + " boxplot(25, 50, 75): " + extremes._1 + " " + extremesNoOutliers._1 + " " + h.quantile(0.25) + " " + h.quantile(0.5) + " " + h.quantile(0.75) + " " + extremesNoOutliers._2 + " " + extremes._2
        println(summary)
        if (write) readmeOut.write(summary + "\n")
      })
      if (write) readmeOut.close()
    } catch {
      case e: Exception => println("error in create dirs: " + name + ". " + e)
    }
  }

  var task: MicroBenchmarkTask = null

  def run(c: Seq[deploylib.mesos.Cluster] = clusters,
          protocol: NSTxProtocol,
          useLogical: Boolean, useFast: Boolean,
          classicDemarcation: Boolean,
          localMasterPercentage: Int, hotspot: Int = 90, localMaster: Int = -1, numItems: Int = 10000): Unit = {
    if (c.size < 1) {
      logger.error("cluster list must not be empty")
    } else if (c.head.slaves.size < 2) {
      logger.error("first cluster must have at least 2 slaves")
    } else {
      task = MicroBenchmarkTask()
      task.schedule(resultClusterAddress, c, protocol, useLogical, useFast, classicDemarcation, localMasterPercentage, hotspot, localMaster, numItems)
    }
  }

}

case class MicroBenchmarkTask()
     extends AvroTask with AvroRecord with TaskBase {
  
  var resultClusterAddress: String = _
  var clusterAddress: String = _
  var numPartitions: Int = _
  var numClusters: Int = _

  def schedule(resultClusterAddress: String,
               clusters: Seq[deploylib.mesos.Cluster],
               protocol: NSTxProtocol,
               useLogicalUpdates: Boolean,
               useFast: Boolean,
               classicDemarcation: Boolean,
               localMasterPercentage: Int,
               hotspot: Int = 90,
               localMaster: Int = -1,
               numItems: Int = 10000,
               partitions: Int = 4): Unit = {
    this.resultClusterAddress = resultClusterAddress

    numPartitions = partitions
    numClusters = clusters.size
    val clientsPerCluster = 1
    val threadsPerClient = 10

    var addlProps = new collection.mutable.ArrayBuffer[(String, String)]()
    var notes = ""

    addlProps.append("scads.mdcc.onEC2" -> "true")

    useFast match {
      case true => {
        addlProps.append("scads.mdcc.fastDefault" -> "true")
        addlProps.append("scads.mdcc.DefaultRounds" -> "1")
        notes += "mdccfast"
      }
      case false => {
        addlProps.append("scads.mdcc.fastDefault" -> "false")
        addlProps.append("scads.mdcc.DefaultRounds" -> "999999999999")
        notes += "mdccclassic"
      }
    }
    classicDemarcation match {
      case true => {
        addlProps.append("scads.mdcc.classicDemarcation" -> "true")
        notes += "_demarcclassic"
      }
      case false => {
        addlProps.append("scads.mdcc.classicDemarcation" -> "false")
        notes += "_demarcquorum"
      }
    }
    addlProps.append("scads.mdcc.localMasterPercentage" -> localMasterPercentage.toString)
    if (protocol == NSTxProtocol2pc()) {
      notes = "2pc"
    } else if (protocol == NSTxProtocolNone()) {
      notes = "qw"
    } else {
      notes += "_local_" + localMasterPercentage
    }

    // percentage of data which is the hotspot (90% of access)
    notes += "_hot_" + hotspot

    if (localMaster != -1) {
      // for localMaster test.
      notes += "_localMaster_" + localMaster
    }

    val addlSettings = new scala.collection.mutable.HashMap[String, String]
    addlSettings.put("hotspot", hotspot.toString)
    addlSettings.put("localMaster", localMaster.toString)

    // true, to test the programming model.
    val progModel = true

    // for spec commit.
//    addlSettings.put("useSpecCommit", "0")

    // for general purpose test, use these options.
//    addlSettings.put("useZipf", "0")
//    addlSettings.put("txMinSize", "1")
//    addlSettings.put("txSizeRange", "4")
//    addlSettings.put("useSpecCommit", "1")  // toggle spec commit


    // admission control settings
    addlSettings.put("useZipf", "0")
    addlSettings.put("txMinSize", "1")
    addlSettings.put("txSizeRange", "1")


    // Start the storage servers.
    val scadsCluster = newMDCCScadsCluster(numPartitions, clusters, addlProps)
    clusterAddress = scadsCluster.root.canonicalAddress

    // Start loaders.
    // usually 150, 10000, for 100 clients
    // try 50000 items? much closer
    // try 100000 items? stick with this.
    // 100 for inconsistency test.
    // try 5000 for probability test. 50 clients.
    //    prob test: 250 ~ 65% commit
    //    prob test: 100 ~ 50% commit
    //    prob test: 20 ~ 20%
    // spec commit: 10000 items, 95% commit, good setting
    //    5000 items
    val loaderTasks = MDCCTpcwMicroLoaderTask(numClusters * numPartitions, clientsPerCluster, numEBs=150, numItems=numItems, numClusters=numClusters, txProtocol=protocol, namespace="items").getLoadingTasks(clusters.head.classSource, scadsCluster.root, addlProps)
    clusters.head.serviceScheduler.scheduleExperiment(loaderTasks)

    var expStartTime: String = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new java.util.Date)

    // Start clients.
    clusters.zipWithIndex.foreach(x => {
//    clusters.take(5).zipWithIndex.foreach(x => {
      val cluster = x._1
      val index = x._2
      val tpcwTasks = MDCCTpcwMicroWorkflowTask(
        numClients=clientsPerCluster,
        executorClass="edu.berkeley.cs.scads.piql.exec.SimpleExecutor",
        numThreads=threadsPerClient,
        iterations=1,
        runLengthMin=3,
        startTime=expStartTime,
        useLogical=useLogicalUpdates,
        clusterId=index,
        numClusters=numClusters,
//        numClusters=5,
        programmingModelTest=progModel,
        microSettings=MicroSettingsFromMap(addlSettings.toMap),
        note=notes).getExperimentTasks(cluster.classSource, scadsCluster.root, resultClusterAddress, addlProps ++ List("scads.comm.externalip" -> "true"))
      cluster.serviceScheduler.scheduleExperiment(tpcwTasks)
    })

  }

  def run(): Unit = {
  }
}
