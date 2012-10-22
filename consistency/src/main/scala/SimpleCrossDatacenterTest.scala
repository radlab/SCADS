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

import tpcw._

import scala.actors.Future
import scala.actors.Futures._

case class Result(var hostname: String,
		  var timestamp: Long) extends AvroPair {
  var rows: Int = _
}

object Experiment extends ExperimentBase {
  val logger = Logger()
  val clusters = getClusters()

  def getClusters() = {
    val regions = List(USWest1, USEast1, EUWest1, APNortheast1, APSoutheast1)
    regions.map(new mesos.Cluster(_))
  }

  def restartClusters(c: Seq[deploylib.mesos.Cluster] = clusters) = {
    c.map(v => future { v.restart }).map(_())
  }

  def updateJars(c: Seq[deploylib.mesos.Cluster] = clusters) = {
    c.map(v => future {v.slaves.pforeach(_.pushJars(MesosCluster.jarFiles))}).map(_())
  }

  def addSlaves(numSlaves: Int, c: Seq[deploylib.mesos.Cluster] = clusters) = {
    c.map(v => future {
      v.addSlaves(numSlaves)
    }).map(_())
  }

  private var actionHistograms: Map[String, scala.collection.mutable.HashMap[String, Histogram]] = null

  private def addHist(aggHist: scala.collection.mutable.HashMap[String, Histogram], key: String, hist: Histogram) = {
    if (aggHist.isDefinedAt(key)) {
      aggHist.put(key, aggHist.get(key).get + hist)
    } else {
      aggHist.put(key, hist)
    }
  }

  def expName(res: tpcw.MDCCResult) = res.startTime + " (" + res.clientConfig.numClusters + "DC." + res.clientConfig.numClients + "." + res.clientConfig.numThreads + "=" + (res.clientConfig.numClusters * res.clientConfig.numClients * res.clientConfig.numThreads) + ")-" + res.loaderConfig.txProtocol.toString.replace("NSTxProtocol", "").replace("()", "")

  // Only gets histograms with something greater than 'name'
  def getActionHistograms(name: String = "2012-01-05 15:55:42.176") = {
    val resultNS = resultCluster.getNamespace[tpcw.MDCCResult]("tpcwMDCCResults")
    val histograms = resultNS.iterateOverRange(None, None)
    .filter(r => expName(r) > name)
    .toSeq
    .groupBy(r => (r.startTime, r.loaderConfig.txProtocol, r.loaderConfig.numClusters))
    .map {
      case( (startTime, txProtocol, numClusters), results) => {
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
    val resultNS = resultCluster.getNamespace[tpcw.MDCCResult]("tpcwMDCCResults")
    val histList = new collection.mutable.ArrayBuffer[tpcw.MDCCResult]()
    val histograms = resultNS.iterateOverRange(None, None).foreach(r => {
      val s = expName(r)
      if (name == s) {
        histList.append(r)
      }
    })
    histList.foreach(resultNS.delete(_))
    println("deleted " + histList.size + " records")
  }

  def writeHistogramCDFMap(name: String) = {
    val mh = actionHistograms(name)
    try {
      val dirs = ("data/" + name + "/").replaceAll(" ", "_")
      (new java.io.File(dirs)).mkdirs

      val readmeFilename = dirs + ("README.txt").replaceAll(" ", "_")
      val readmeOut = new java.io.BufferedWriter(
        new java.io.FileWriter(readmeFilename))

      mh.foreach(t => {
        val n = t._1
        val h = t._2
        var total:Long = 0
        val totalRequests = h.totalRequests

        val filename = dirs + ("tpcw_" + n + ".csv").replaceAll(" ", "_")
        val out = new java.io.BufferedWriter(new java.io.FileWriter(filename))
        ((0 until h.buckets.length).map(_ * h.bucketSize) zip h.buckets).foreach(x => {
          total = total + x._2
          out.write(" " + x._1 + ", " + total * 100.0 / totalRequests.toFloat + "\n")
        })
        out.close()

        val rawFilename = dirs + ("RAW_tpcw_" + n + ".csv").replaceAll(" ", "_")
        val rawOut = new java.io.BufferedWriter(new java.io.FileWriter(rawFilename))
        ((0 until h.buckets.length).map(_ * h.bucketSize) zip h.buckets).foreach(x => {
          rawOut.write(" " + x._1 + ", " + x._2 + "\n")
        })
        rawOut.close()

        val summary = n + ": requests: " + totalRequests + " 50%: " + h.quantile(.5) + " 90%: " + h.quantile(.9) + " 95%: " + h.quantile(.95) + " 99%: " + h.quantile(.99) + " avg: " + h.average
        println(summary)
        readmeOut.write(summary + "\n")
      })
      readmeOut.close()
    } catch {
      case e: Exception => println("error in create dirs: " + name + ". " + e)
    }
  }

  var task: Task = null

  def run(c: Seq[deploylib.mesos.Cluster] = clusters,
          protocol: NSTxProtocol = NSTxProtocolNone()): Unit = {
//    stopCluster
    if (c.size < 1) {
      logger.error("cluster list must not be empty")
    } else if (c.head.slaves.size < 2) {
      logger.error("first cluster must have at least 2 slaves")
    } else {
      task = Task()
      task.schedule(resultClusterAddress, c, protocol)
    }
  }

  def stopCluster() = {
    if (task != null) {
      task.stopCluster
      task = null
    }
  }
}

case class Task()
     extends AvroTask with AvroRecord with TaskBase {
  
  var resultClusterAddress: String = _
  var clusterAddress: String = _
  var numPartitions: Int = _
  var numClusters: Int = _

  def schedule(resultClusterAddress: String, clusters: Seq[deploylib.mesos.Cluster], protocol: NSTxProtocol, partitions: Int = 1): Unit = {
    this.resultClusterAddress = resultClusterAddress

    numPartitions = partitions
    numClusters = clusters.size
    val clientsPerCluster = 6
    val threadsPerClient = 5

    var addlProps = new collection.mutable.ArrayBuffer[(String, String)]()

    // Using fast.
    addlProps.append("scads.mdcc.fastDefault" -> "true")
    addlProps.append("scads.mdcc.DefaultRounds" -> "1")

    // Megastore is classic.
//    addlProps.append("scads.mdcc.fastDefault" -> "false")
//    addlProps.append("scads.mdcc.DefaultRounds" -> "999999999999")

    // Use quorum demarcation.
    addlProps.append("scads.mdcc.classicDemarcation" -> "false")

    // Try not setting this for the metadata to be the same as before.
    // for regular mdcc, should be commented out.
    // megastore needs this to be true.
//    addlProps.append("scads.mdcc.onEC2" -> "true")

    // All masters are in ap-southeast. use this for mdcc.
    addlProps.append("scads.mdcc.localMasterPercentage" -> "-1")

    // mdcc cannot use random. but maybe helps with none, 2pc?
//    addlProps.append("scads.mdcc.localMasterPercentage" -> "-2")

    // Megastore is all local.
//    addlProps.append("scads.mdcc.localMasterPercentage" -> "100")

    // For profiling??
    // http://pedanttinen.blogspot.com/2012/02/remote-visualvm-session-through-ssh.html
//    addlProps.append("com.sun.management.jmxremote.port" -> "9999")
//    addlProps.append("com.sun.management.jmxremote.ssl" -> "false")
//    addlProps.append("com.sun.management.jmxremote.authenticate" -> "false")

    // more actor threads?
    addlProps.append("actors.corePoolSize" -> "75")
    addlProps.append("actors.maxPoolSize" -> "200")

    // Start the storage servers.
    val scadsCluster = newMDCCScadsCluster(numPartitions, clusters, addlProps)
    clusterAddress = scadsCluster.root.canonicalAddress

    // Start loaders.
    // Usually 75 ebs, 5000 items, for 50 clients.
    // Usually 150 ebs, 10000 items, for 100 clients.
    // try 300 ebs, 20000 items for 200 clients.
    // try 450 ebs, 30000 items for 300 clients.
    // try 600 ebs, 40000 items for 400 clients.
    // try 37 ebs, 2500 items for Megastore.
    // Usually 10 loaders. trying 5.
    // For load test, 150, 5000
    val loaderTasks = MDCCTpcwLoaderTask(numClusters * numPartitions, clientsPerCluster, numEBs=150, numItems=10000, numClusters=numClusters, txProtocol=protocol).getLoadingTasks(clusters.head.classSource, scadsCluster.root, addlProps)
    clusters.head.serviceScheduler.scheduleExperiment(loaderTasks)


    val startTime: String = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new java.util.Date)

    // Start clients.
    clusters.zipWithIndex.foreach(x => {
//    List(clusters.head).zipWithIndex.foreach(x => {  // Megastore
      val cluster = x._1
      val index = x._2
      val tpcwTasks = MDCCTpcwWorkflowTask(
        numClients=clientsPerCluster,
//        numClients=1,  // Megastore
        executorClass="edu.berkeley.cs.scads.piql.exec.SimpleExecutor",
        startTime=startTime,
        numThreads=threadsPerClient,
        runLengthMin=2,
        clusterId=index,
        numClusters=numClusters).getExperimentTasks(cluster.classSource, scadsCluster.root, resultClusterAddress, addlProps ++ List("scads.comm.externalip" -> "true"))
//        numClusters=1).getExperimentTasks(cluster.classSource, scadsCluster.root, resultClusterAddress, addlProps ++ List("scads.comm.externalip" -> "true"))  // Megastore

      cluster.serviceScheduler.scheduleExperiment(tpcwTasks)
    })

  }

  def stopCluster() = {
    val cluster = new ExperimentalScadsCluster(ZooKeeperNode(clusterAddress))
    cluster.shutdown
  }

  def run(): Unit = {
  }

}
