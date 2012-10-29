package edu.berkeley.cs
package scads
package piql
package mviews

import avro.runtime._
import org.apache.avro.file.CodecFactory
import java.io.File

import deploylib.mesos._
import tpcw._
import tpcw.scale._
import perf._
import comm._
import storage._
import piql.debug.DebugExecutor
import avro.marker.{AvroPair, AvroRecord}
import edu.berkeley.cs.scads.perf.ReplicatedExperimentTask
import java.util.concurrent.TimeUnit
import net.lag.logging.Logger
import org.apache.avro.generic.GenericContainer


case class RefreshResult(var expId: String,
                        var function: String) extends AvroPair {
  var times: Seq[Long] = Nil
}

case class TpcwViewRefreshTask(var experimentAddress: String,
                               var clusterAddress: String,
                               var resultClusterAddress: String,
                               var expId: String) extends AvroRecord with ExperimentTask {


  def run() = {
    val clusterRoot = ZooKeeperNode(clusterAddress)
    val coordination = clusterRoot.getOrCreate("coordination/clients")

    val resultCluster = new ScadsCluster(ZooKeeperNode(resultClusterAddress))
    val results = resultCluster.getNamespace[RefreshResult]("updateResults")
    logger.info("Waiting for experiment to start.")
    clusterRoot.awaitChild("clusterReady")
    logger.info("Cluster ready... entering refresh loop")

    val cluster = new ScadsCluster(clusterRoot)
    val client = new TpcwClient(cluster, new ParallelExecutor)

    val ocTimes = new scala.collection.mutable.ArrayBuffer[Long]()
    val rcTimes = new scala.collection.mutable.ArrayBuffer[Long]()

    logger.info("Waiting for clients to start")
    coordination.awaitChild("runViewRefresh")

    while(coordination.get("expRunning").isDefined) {
      logger.info("Updating OrderCounts")
      val ocStartTime = System.currentTimeMillis()
      retry(5) { client.updateOrderCount() }
      val ocEndTime = System.currentTimeMillis()
      logger.info("OrderCount Update complete in %d", ocEndTime - ocStartTime)
      ocTimes += (ocEndTime - ocStartTime)

      logger.info("Updating RelatedCounts")
      val rcStartTime = System.currentTimeMillis()
      retry(5) { client.updateRelatedCounts() }
      val rcEndTime = System.currentTimeMillis()
      logger.info("Related Counts updated in %d", rcEndTime - rcStartTime)
      rcTimes += (rcEndTime - rcStartTime)

      coordination.getOrCreate("viewsReady")
      val nextEpoch = client.calculateEpochs().drop(1).head
      while(System.currentTimeMillis < nextEpoch) {
        val sleepTime = System.currentTimeMillis - nextEpoch
        if(sleepTime > 0) Thread.sleep(sleepTime)
      }
    }

    logger.info("Recording Results")
    val ocResult = RefreshResult(expId, "orderCount")
    ocResult.times = ocTimes
    val rcResult = RefreshResult(expId, "relatedCount")
    rcResult.times = rcTimes
    results ++= Seq(ocResult, rcResult)
  }
}

object TpcwScaleExperiment {
  val logger = Logger()
  var resultClusterAddress = ZooKeeperNode("zk://zoo1.millennium.berkeley.edu,zoo2.millennium.berkeley.edu,zoo3.millennium.berkeley.edu/home/marmbrus/sigmod2013")
  val resultsCluster = new ScadsCluster(resultClusterAddress)
  val scaleResults =  resultsCluster.getNamespace[Result]("tpcwScaleResults")
  val updateResults = resultsCluster.getNamespace[RefreshResult]("updateResults")


  implicit def productSeqToExcel(lines: Seq[Product]) = new {
    import java.io._
    def toExcel: Unit = {
      val file = File.createTempFile("scadsOut", ".csv")
      val writer = new FileWriter(file)

      lines.map(_.productIterator.mkString(",") + "\n").foreach(writer.write)
      writer.close

      Runtime.getRuntime.exec(Array("/usr/bin/open", file.getCanonicalPath))
    }
  }

  implicit def avroIterWriter[RecordType <: GenericContainer](iter: Iterator[RecordType]) = new {
     def toAvroFile(file: File, codec: Option[CodecFactory] = None)(implicit schema: TypedSchema[RecordType]) = {
       val outfile = AvroOutFile[RecordType](file, codec)
       iter.foreach(outfile.append)
       outfile.close
      }
    }


  def backup = scaleResults.iterateOverRange(None,None).toAvroFile(new java.io.File("scaleResults" + System.currentTimeMillis() + ".avro"))

  implicit def toOption[A](a: A) = Option(a)

  lazy val testTpcwClient =
    new piql.tpcw.TpcwClient(new piql.tpcw.TpcwLoaderTask(10,5,10,10000,2).newTestCluster, new ParallelExecutor with DebugExecutor)

  lazy val tinyTpcwClient =
    new piql.tpcw.TpcwClient(new piql.tpcw.TpcwLoaderTask(1,1,1,1000,1).newTestCluster, new ParallelExecutor with DebugExecutor)

  def resultsByAction = scaleResults
    .iterateOverRange(None,None)
    .filter(_.iteration != 1)
    .flatMap(_.times).toSeq
    .groupBy(_.name)
    .map { case (name, hists) => (name, hists.reduceLeft(_ + _).quantile(0.99)) }

  def results = {
    val allResults = scaleResults.iterateOverRange(None,None).toSeq

    allResults.filter(_.iteration != 1)
      .groupBy(r => (r.loaderConfig.numServers, r.clientConfig.expId)).toSeq
      .sortBy(_._1)
      .foreach {
      case (size, results) =>
        results.flatMap(_.times)
          .groupBy(_.name).toSeq
          .sortBy(_._1)
          .foreach {
          case (wi, hists) =>
            val aggResults = hists.reduceLeft(_+_)
          println(Seq(size._1, size._2, wi, aggResults.quantile(0.90), aggResults.quantile(0.99), aggResults.stddev, aggResults.totalRequests).mkString("\t"))
        }
    }
  }

  def runScaleTest(numServers: Int, executor: String = "edu.berkeley.cs.scads.piql.exec.ParallelExecutor")(implicit cluster: Cluster) = {
    val (scadsTasks, scadsCluster) = TpcwLoaderTask(numServers, numServers/2, replicationFactor=2, numEBs = 150 * numServers/2, numItems = 10000).delayedCluster

    val tpcwTaskTemplate = TpcwWorkflowTask(
          numServers/2,
          executor,
          iterations = 5,
          runLengthMin = 5
        )

    val tpcwTasks = tpcwTaskTemplate.delayedSchedule(scadsCluster, resultsCluster)

    val viewRefreshTask = TpcwViewRefreshTask(
      tpcwTaskTemplate.experimentAddress,
      tpcwTaskTemplate.clusterAddress,
      tpcwTaskTemplate.resultClusterAddress,
      tpcwTaskTemplate.expId
    ).toJvmTask

    cluster.serviceScheduler.scheduleExperiment(scadsTasks ++ tpcwTasks :+ viewRefreshTask)

    logger.info("Experiment started on cluster %s", scadsCluster.root.canonicalAddress)

    new ComputationFuture[TpcwClient] {
      /**
       * The computation that should run with this future. Is guaranteed to only
       * be called at most once. Timeout hint is a hint for how long the user is
       * willing to wait for this computation to compute
       */
      protected def compute(timeoutHint: Long, unit: TimeUnit) = {
        scadsCluster.root.awaitChild("clusterReady")
        new TpcwClient(scadsCluster, new ParallelExecutor)
      }

      /**
       * Signals a request to cancel the computation. Is guaranteed to only be
       * called at most once.
       */
      protected def cancelComputation {}
    }
  }
}
