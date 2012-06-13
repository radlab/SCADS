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

import edu.berkeley.cs.scads.storage.transactions.mdcc.MDCCMetaDefault

import scala.actors.Future
import scala.actors.Futures._

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap

import tpcw._

object FailureTest extends ExperimentBase {
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

  private var actionHistograms: Map[String, HashMap[Int, Seq[Int]]] = null

  private def mergeLists(a: Seq[Int], b: Seq[Int]) = {
    val aa = a.zipWithIndex.map(x => (((x._2 + 1).toDouble / (a.size + 1).toDouble), x._1))
    val bb = b.zipWithIndex.map(x => (((x._2 + 1).toDouble / (b.size + 1).toDouble), x._1))
    (aa ++ bb).sortWith((i, j) => i._1 < j._1).map(_._2)
  }

  private def mergeMaps(aggMap: HashMap[Int, Seq[Int]], newMap: Map[String, Seq[Int]]) = {
    val allKeys = (newMap.keys.map(_.toInt) ++ aggMap.keys).toSet
    val newAggMap = allKeys.map(k => (aggMap.get(k), newMap.get(k.toString)) match {
      case (Some(a), Some(b)) => (k, mergeLists(a, b))
      case (Some(a), None) => (k, a)
      case (None, Some(b)) => (k, b)
      case _ => (k, Nil)
    }).toList
    
    val res = new HashMap[Int, Seq[Int]]()
    res ++= newAggMap
    res
  }

  def expName(startTime: String, prot: NSTxProtocol, log: Boolean, note: String) = {
    val logical = if (log) "logical" else "physical"
    startTime + " " + prot + " " + logical + " " + note
  }

  // Only gets histograms with something greater than 'name'
  def getActionHistograms(name: String = "2012-01-05 15:55:42.176") = {
    val resultNS = resultCluster.getNamespace[MDCCFailureResult]("FailureResults")
    val histograms = resultNS.iterateOverRange(None, None)
    .filter(r => expName(r.startTime, r.loaderConfig.txProtocol, r.clientConfig.useLogical, r.clientConfig.note) > name)
    .toSeq
    .groupBy(r => (r.startTime, r.loaderConfig.txProtocol, r.clientConfig.useLogical, r.clientConfig.note))
    .map {
      case( (startTime, txProtocol, logical, note), results) => {
        var aggHist = new HashMap[Int, Seq[Int]]
        println(expName(startTime, txProtocol, logical, note) + "      " + results.size)

        results.foreach(r => {
          aggHist = mergeMaps(aggHist, r.times)
        })

        (expName(startTime, txProtocol, logical, note), aggHist)
      }
    }.toList.toMap

    println("sorted keys: ")
    histograms.keys.toList.sortWith(_.compare(_) < 0).foreach(println _)

    actionHistograms = histograms
    histograms
  }

  def deleteHistograms(name: String) = {
    val resultNS = resultCluster.getNamespace[MDCCFailureResult]("FailureResults")
    val histList = new collection.mutable.ArrayBuffer[MDCCFailureResult]()
    val histograms = resultNS.iterateOverRange(None, None).foreach(r => {
      val s = expName(r.startTime, r.loaderConfig.txProtocol, r.clientConfig.useLogical, r.clientConfig.note)
      if (name == s) {
        histList.append(r)
      }
    })
    histList.foreach(resultNS.delete(_))
    println("deleted " + histList.size + " records")
  }

  def writeHistogramCDFMap(name: String, write: Boolean = true, bucketSize: Int = 10) = {
    val mh = actionHistograms(name)
    try {
      val dirs = ("failure_data/" + name + "/").replaceAll(" ", "_")
      if (write) (new java.io.File(dirs)).mkdirs

      val filename = dirs + ("failure.csv").replaceAll(" ", "_")
      val out =
        if (write)
          new java.io.BufferedWriter(new java.io.FileWriter(filename))
        else
          null
      mh.toList.sortWith((a, b) => a._1 < b._1).foreach(t => {
        val bucket = t._1
        val l = t._2
        val length = l.size.toFloat
        val l2 = l.zipWithIndex.map(x => (bucket + (x._2.toFloat / length) * bucketSize.toFloat, x._1))
//        val l2 = l.zipWithIndex.map(x => (bucket + ((x._2.toFloat / length) * bucketSize.toFloat * 1000).toInt, x._1))
        l2.foreach(x => {
          if (write) out.write(" " + x._1 + ", " + x._2 + "\n")
        })
      })
      if (write) out.close()
    } catch {
      case e: Exception => println("error in create dirs: " + name)
    }
  }

  var task: FailureTestTask = null

  def run(c: Seq[deploylib.mesos.Cluster] = clusters,
          protocol: NSTxProtocol,
          useLogical: Boolean, useFast: Boolean,
          classicDemarcation: Boolean,
          localMasterPercentage: Int): Unit = {
    if (c.size < 1) {
      logger.error("cluster list must not be empty")
    } else if (c.head.slaves.size < 2) {
      logger.error("first cluster must have at least 2 slaves")
    } else {
      task = FailureTestTask()
      task.schedule(resultClusterAddress, c, protocol, useLogical, useFast, classicDemarcation, localMasterPercentage)
      println("task.clusterAddress: " + task.clusterAddress)
    }
  }

  def sendIgnoreMessage(clusterAddress: String, dc: String = "compute-1") = {
    val l = MDCCMetaDefault.getServiceList(ZooKeeperNode(clusterAddress + "/namespaces/microItems"), dc)
    implicit val rs = RemoteService[StorageMessage](RemoteNode("fake.ip", 9999), ServiceNumber(9999))
    l.foreach(s => {
      println("sending ignore message to: " + s)
      s ! IgnoreAllMessages()
    })
  }

}

case class FailureTestTask()
     extends AvroTask with AvroRecord with TaskBase {
  
  var resultClusterAddress: String = _
  var clusterAddress: String = _
  var numPartitions: Int = _
  var numClusters: Int = _
  var scadsClusterRootPath: String = _

  def schedule(resultClusterAddress: String, clusters: Seq[deploylib.mesos.Cluster], protocol: NSTxProtocol, useLogicalUpdates: Boolean, useFast: Boolean, classicDemarcation: Boolean, localMasterPercentage: Int): Unit = {
    this.resultClusterAddress = resultClusterAddress

    val firstSize = clusters.head.slaves.size - 1
    numPartitions = (clusters.tail.map(_.slaves.size) ++ List(firstSize)).min
    numClusters = clusters.size


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
    if (protocol != NSTxProtocolMDCC()) {
      notes = "2pc"
    } else {
      notes += "_local_" + localMasterPercentage
    }

    // Start the storage servers.
    val scadsCluster = newMDCCScadsCluster(numPartitions, clusters, addlProps)
    scadsClusterRootPath = scadsCluster.root.path
    clusterAddress = scadsCluster.root.canonicalAddress

    // Start loaders.
    val loaderTasks = MDCCTpcwMicroLoaderTask(numClusters * numPartitions, 2, numEBs=150, numItems=10000, numClusters=numClusters, txProtocol=protocol, namespace="items").getLoadingTasks(clusters.head.classSource, scadsCluster.root, addlProps)
    clusters.head.serviceScheduler.scheduleExperiment(loaderTasks)

    var expStartTime: String = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new java.util.Date)

    // Start clients.
    val tpcwTasks = MDCCFailureWorkflowTask(
      numClients=15,
      executorClass="edu.berkeley.cs.scads.piql.exec.SimpleExecutor",
      numThreads=7,
      runLengthMin=4,
      startTime=expStartTime,
      useLogical=useLogicalUpdates,
      note=notes).getExperimentTasks(clusters.head.classSource, scadsCluster.root, resultClusterAddress, addlProps ++ List("scads.comm.externalip" -> "true"))
    clusters.head.serviceScheduler.scheduleExperiment(tpcwTasks)

  }

  def run(): Unit = {
  }
}
