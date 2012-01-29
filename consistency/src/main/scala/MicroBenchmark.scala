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

object MicroBenchmark extends ExperimentBase {
  val logger = Logger()
  val clusters = getClusters()

  def getClusters() = {
    val regions = List(USWest1, USEast1, EUWest1, APNortheast1, APSoutheast1)
    regions.map(new mesos.Cluster(_))
  }

  def restartClusters(c: Seq[deploylib.mesos.Cluster] = clusters) = {
    c.map(v => new Future(v.restart)).map(_())
  }

  def updateJars(c: Seq[deploylib.mesos.Cluster] = clusters) = {
    c.map(v => new Future(v.slaves.pforeach(_.pushJars(MesosCluster.jarFiles)))).map(_())
  }

  private var actionHistograms: Map[String, scala.collection.mutable.HashMap[String, Histogram]] = null

  private def addHist(aggHist: scala.collection.mutable.HashMap[String, Histogram], key: String, hist: Histogram) = {
    if (aggHist.isDefinedAt(key)) {
      aggHist.put(key, aggHist.get(key).get + hist)
    } else {
      aggHist.put(key, hist)
    }
  }

  def expName(startTime: String, prot: NSTxProtocol, log: Boolean, note: String) = {
    val logical = if (log) "logical" else "physical"
    startTime + " " + prot + " " + logical + " " + note
  }

  // Only gets histograms with something greater than 'name'
  def getActionHistograms(name: String = "2012-01-05 15:55:42.176") = {
    val resultNS = resultCluster.getNamespace[MDCCMicroBenchmarkResult]("MicroBenchmarkResults")
    val histograms = resultNS.iterateOverRange(None, None)
    .filter(r => expName(r.startTime, r.loaderConfig.txProtocol, r.clientConfig.useLogical, r.clientConfig.note) > name)
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
        println(expName(startTime, txProtocol, logical, note) + "      " + results.size)
        (expName(startTime, txProtocol, logical, note), aggHist)
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
      val s = expName(r.startTime, r.loaderConfig.txProtocol, r.clientConfig.useLogical, r.clientConfig.note)
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
      val dirs = ("micro_data/" + name + "/").replaceAll(" ", "_")
      (new java.io.File(dirs)).mkdirs

      mh.foreach(t => {
        val n = t._1
        val h = t._2
        val totalRequests = h.totalRequests
        val filename = dirs + ("micro_" + n + ".csv").replaceAll(" ", "_")
        try {
          var total:Long = 0
          val out = new java.io.BufferedWriter(new java.io.FileWriter(filename))
          ((1 to h.buckets.length).map(_ * h.bucketSize) zip h.buckets).foreach(x => {
            total = total + x._2
            out.write(" " + x._1 + ", " + total * 100.0 / totalRequests.toFloat + "\n")
          })
          out.close()
          println(n + ": requests: " + totalRequests + " 50%: " + h.quantile(.5) + " 90%: " + h.quantile(.9) + " 95%: " + h.quantile(.95) + " 99%: " + h.quantile(.99) + " avg: " + h.average)
        } catch {
          case e: Exception =>
            println("error in writing file: " + filename)
        }
      })
    } catch {
      case e: Exception => println("error in create dirs: " + name)
    }
  }

  var task: MicroBenchmarkTask = null

  def run(c: Seq[deploylib.mesos.Cluster] = clusters,
          protocol: NSTxProtocol,
          useLogical: Boolean, useFast: Boolean, classicDemarcation: Boolean): Unit = {
    if (c.size < 1) {
      logger.error("cluster list must not be empty")
    } else if (c.head.slaves.size < 2) {
      logger.error("first cluster must have at least 2 slaves")
    } else {
      task = MicroBenchmarkTask()
      task.schedule(resultClusterAddress, c, protocol, useLogical, useFast, classicDemarcation)
    }
  }

}

case class MicroBenchmarkTask()
     extends AvroTask with AvroRecord with TaskBase {
  
  var resultClusterAddress: String = _
  var clusterAddress: String = _
  var numPartitions: Int = _
  var numClusters: Int = _

  def schedule(resultClusterAddress: String, clusters: Seq[deploylib.mesos.Cluster], protocol: NSTxProtocol, useLogicalUpdates: Boolean, useFast: Boolean, classicDemarcation: Boolean): Unit = {
    this.resultClusterAddress = resultClusterAddress

    val firstSize = clusters.head.slaves.size - 1
    numPartitions = (clusters.tail.map(_.slaves.size) ++ List(firstSize)).min
    numClusters = clusters.size


    var addlProps = new collection.mutable.ArrayBuffer[(String, String)]()
    var notes = ""

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
    if (protocol != NSTxProtocolMDCC()) {
      notes = "2pc"
    }

    // Start the storage servers.
    val scadsCluster = newMDCCScadsCluster(numPartitions, clusters, addlProps)
    clusterAddress = scadsCluster.root.canonicalAddress

    // Start loaders.
    val loaderTasks = MDCCTpcwMicroLoaderTask(numClusters * numPartitions, 2, numEBs=150, numItems=10000, numClusters=numClusters, txProtocol=protocol, namespace="items").getLoadingTasks(clusters.head.classSource, scadsCluster.root)
    clusters.head.serviceScheduler.scheduleExperiment(loaderTasks)

    var expStartTime: String = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new java.util.Date)

    // Start clients.
    val tpcwTasks = MDCCTpcwMicroWorkflowTask(
      numClients=2,
      executorClass="edu.berkeley.cs.scads.piql.exec.SimpleExecutor",
      numThreads=2,
      iterations=1,
      runLengthMin=5,
      startTime=expStartTime,
      useLogical=useLogicalUpdates,
      note=notes).getExperimentTasks(clusters.head.classSource, scadsCluster.root, resultClusterAddress)
    clusters.head.serviceScheduler.scheduleExperiment(tpcwTasks)

    // Start the task.
//    val task1 = this.toJvmTask(clusters.head.classSource)
//    clusters.head.serviceScheduler.scheduleExperiment(task1 :: Nil)
  }

  def run(): Unit = {
  }
}
