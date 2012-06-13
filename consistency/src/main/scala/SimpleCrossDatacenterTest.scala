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

  def expName(startTime: String, numClusters: Int, prot: NSTxProtocol) = startTime + " (" + numClusters + ") : " + prot

  // Only gets histograms with something greater than 'name'
  def getActionHistograms(name: String = "2012-01-05 15:55:42.176 (5) : NSTxProtocolNone()") = {
    val resultNS = resultCluster.getNamespace[tpcw.MDCCResult]("tpcwMDCCResults")
    val histograms = resultNS.iterateOverRange(None, None)
    .filter(r => expName(r.startTime, r.loaderConfig.numClusters, r.loaderConfig.txProtocol) > name)
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
        println(expName(startTime, numClusters, txProtocol) + "      " + results.size)
        (expName(startTime, numClusters, txProtocol), aggHist)
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
      val s = expName(r.startTime, r.loaderConfig.numClusters, r.loaderConfig.txProtocol)
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

      mh.foreach(t => {
        val n = t._1
        val h = t._2
        val totalRequests = h.totalRequests
        val filename = dirs + ("tpcw_" + n + ".csv").replaceAll(" ", "_")
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

  var task: Task = null

  def run(c: Seq[deploylib.mesos.Cluster] = clusters,
          protocol: NSTxProtocol = NSTxProtocolNone()): Unit = {
    stopCluster
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

case class KeyRec(var x: Int) extends AvroRecord

case class ValueRec(var s: String,
                    @FieldGT(1)
                    @FieldGE(2)
                    @FieldLT(3)
                    @FieldLE(4)
                    var i: Int,
                    @FieldGT(1)
                    @FieldGE(1)
                    @FieldLT(4)
                    @FieldLE(4)
                    var a: Long,
                    var b: Float,
                    var c: Double) extends AvroRecord

case class Task()
     extends AvroTask with AvroRecord with TaskBase {
  
  var resultClusterAddress: String = _
  var clusterAddress: String = _
  var numPartitions: Int = _
  var numClusters: Int = _

  def schedule(resultClusterAddress: String, clusters: Seq[deploylib.mesos.Cluster], protocol: NSTxProtocol): Unit = {
    this.resultClusterAddress = resultClusterAddress

    val firstSize = clusters.head.slaves.size - 1
    numPartitions = (clusters.tail.map(_.slaves.size) ++ List(firstSize)).min
    numClusters = clusters.size

    // Start the storage servers.
    val scadsCluster = newMDCCScadsCluster(numPartitions, clusters)
    clusterAddress = scadsCluster.root.canonicalAddress

    // Start loaders.
    val loaderTasks = MDCCTpcwLoaderTask(numClusters * numPartitions, 15, numEBs=150, numItems=10000, numClusters=numClusters, txProtocol=protocol).getLoadingTasks(clusters.head.classSource, scadsCluster.root)
    clusters.head.serviceScheduler.scheduleExperiment(loaderTasks)

    // Start clients.
    val tpcwTasks = MDCCTpcwWorkflowTask(
      numClients=15,
      executorClass="edu.berkeley.cs.scads.piql.exec.SimpleExecutor",
      numThreads=7,
      iterations=1,
      runLengthMin=5).getExperimentTasks(clusters.head.classSource, scadsCluster.root, resultClusterAddress)
    clusters.head.serviceScheduler.scheduleExperiment(tpcwTasks)

    // Start the task.
//    val task1 = this.toJvmTask(clusters.head.classSource)
//    clusters.head.serviceScheduler.scheduleExperiment(task1 :: Nil)
  }

  def stopCluster() = {
    val cluster = new ExperimentalScadsCluster(ZooKeeperNode(clusterAddress))
    cluster.shutdown
  }

  def run(): Unit = {
    val logger = Logger()
    val cluster = new ExperimentalScadsCluster(ZooKeeperNode(clusterAddress))
    cluster.blockUntilReady(numPartitions * numClusters)

    val serversByCluster = (0 until numClusters).map(i => cluster.getAvailableServers("cluster-" + i))

    val serversByPartition = (0 until numPartitions).map(i => serversByCluster.map(_(i)))

//    val resultCluster = new ScadsCluster(ZooKeeperNode(resultClusterAddress))
//    val results = resultCluster.getNamespace[Result]("singleDataCenterTest")

    val ns = new SpecificNamespace[KeyRec, ValueRec]("testns", cluster, cluster.namespaces) with Transactions[KeyRec, ValueRec] {
      override lazy val protocolType = NSTxProtocol2pc()
    }
    ns.open()
    ns.setPartitionScheme(List((None, cluster.getAvailableServers)))

    // Load data in a tx to get default metadata.
    new Tx(100) ({
      ns.put(KeyRec(1), ValueRec("A", 1, 1, 1.0.floatValue, 1.0))
      ns.put(KeyRec(2), ValueRec("B", 1, 1, 1.0.floatValue, 1.0))
      ns.put(KeyRec(3), ValueRec("C", 1, 1, 1.0.floatValue, 1.0))
      ns.put(KeyRec(4), ValueRec("D", 1, 1, 1.0.floatValue, 1.0))
    }).Execute()

    val tx1 = new Tx(100) ({
      List.range(5, 5 + 4).foreach(x => ns.put(KeyRec(x),
                                               ValueRec("G", 1, 1, 1.0.floatValue, 1.0)))
    }).Execute()

    val tx2 = new Tx(100) ({
      List.range(9, 9 + 4).foreach(x => ns.put(KeyRec(x),
                                               ValueRec("H", 1, 1, 1.0.floatValue, 1.0)))
    }).Execute()

    val tx3 = new Tx(100) ({
      List.range(7, 7 + 4).foreach(x => {
        ns.get(KeyRec(x))
        ns.put(KeyRec(x), ValueRec("I", 1, 1, 1.0.floatValue, 1.0))})
    }).Execute()

    println("delete 1")
    val tx4 = new Tx(100) ({
      // need to read your writes...
      ns.get(KeyRec(1))
      ns.put(KeyRec(1), None)
    }).Execute()

    println("running some logical updates")
    val tx5 = new Tx(100) ({
      ns.putLogical(KeyRec(12), ValueRec("", 2, 3, 2.1.floatValue, 0.2))
    })
    tx5.Execute()
    tx5.Execute()
    tx5.Execute()
    tx5.Execute()
    tx5.Execute()
    ns.getRange(None, None).foreach(x => println(x))

//    val hostname = java.net.InetAddress.getLocalHost.getHostName
//    val result = Result(hostname, System.currentTimeMillis)
//    result.rows = ns.getRange(None, None).size
//    results.put(result)

    cluster.shutdown
  }
}
