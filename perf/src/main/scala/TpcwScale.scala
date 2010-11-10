package edu.berkeley.cs
package scads
package perf
package tpcw
package scale

import comm._
import piql._
import storage._
import avro.runtime._
import avro.marker._

import deploylib._
import deploylib.mesos._

import scala.util.Random
import scala.collection.{ mutable => mu }

case class ResultKey(var clientConfig: LoadClient, var loaderConfig: TpcwLoaderClient, var clusterAddress: String, var clientId: Int, var iteration: Int, var threadId: Int) extends AvroRecord
case class ResultValue(var totalElaspedTime: Long /* in ms */, var times: Histogram, var skips: Int) extends AvroRecord

object TpcwScaleExperiment extends Experiment {
  val results = resultCluster.getNamespace[ResultKey, ResultValue]("tpcwScale")

  def clear = results.getRange(None, None).foreach(r => results.put(r._1, None))

  def printResults: Unit = {
    val runs = results.getRange(None, None).groupBy(k => (k._1.clientConfig, k._1.iteration)).filterNot(_._1._2 == 1).values
    println(List("numServers", "execClass", "EB", "totalReqs", "50thPercentile", "99thPercentile", "99.9thPercentile").mkString("\t"))
    runs.foreach(run => {
      val totalRequests = run.map(_._2.times.buckets.sum).sum
      val aggregrateHistogram = run.map(_._2.times).reduceLeft(_ + _)
      val cumulativeHistogram = aggregrateHistogram.buckets.scanLeft(0L)(_ + _).drop(1)
      val quantile50ResponseTime = cumulativeHistogram.findIndexOf(_ >= totalRequests * 0.50) * aggregrateHistogram.bucketSize
      val quantile99ResponseTime = cumulativeHistogram.findIndexOf(_ >= totalRequests * 0.99) * aggregrateHistogram.bucketSize
      val quantile999ResponseTime = cumulativeHistogram.findIndexOf(_ >= totalRequests * 0.999) * aggregrateHistogram.bucketSize

      println(List(run.head._1.loaderConfig.numServers, run.head._1.clientConfig.executorClass, run.head._1.loaderConfig.numEBs, totalRequests, quantile50ResponseTime, quantile99ResponseTime, quantile999ResponseTime).mkString("\t"))
    })
  }

  /**
   * Make a graph point using `size` servers, `size` clients, `size` loaders,
   * `ceil(numEBs / size)` threads per client, and `numItems` items
   */
  def makeGraphPoint(size: Int, numEBs: Double, numItems: Int = 1000)(implicit classpath: Seq[ClassSource], scheduler: ExperimentScheduler, zookeeper: ZooKeeperProxy#ZooKeeperNode) = {
    require(size > 0 && numEBs > 0.0 && numItems > 0)
    val numThreads = scala.math.ceil(numEBs / size.toDouble).toInt
    logger.info("%d clients and %f EBs will result in %d threads per client node", size, numEBs, numThreads)
    val cluster = TpcwLoaderClient(size, size, numEBs, numItems).newCluster(size, size)
    LoadClient(size, "edu.berkeley.cs.scads.piql.SimpleExecutor").schedule(cluster)
    cluster
  }

}

case class LoadClient(var numClients: Int,
                      var executorClass: String,
                      var iterations: Int = 3,
                      var runLengthMin: Int = 5) extends AvroRecord with ReplicatedAvroClient {

  def run(clusterRoot: ZooKeeperProxy#ZooKeeperNode) = {
    val coordination = clusterRoot.getOrCreate("coordination/clients")
    val cluster = new ScadsCluster(clusterRoot)
    var executor = Class.forName(executorClass).newInstance.asInstanceOf[QueryExecutor]
    val tpcwClient = new TpcwClient(cluster, executor)

    val clientId = coordination.registerAndAwait("clientStart", numClients)

    logger.info("Waiting for cluster to be ready")
    val clusterConfig = clusterRoot.awaitChild("clusterReady")
    val loaderConfig = classOf[TpcwLoaderClient].newInstance.parse(clusterConfig.data)

    val threads = scala.math.ceil(loaderConfig.numEBs / numClients.toDouble).toInt
    logger.info("client going to use %d threads (1 EB per thread)", threads)

    val loader = new TpcwLoader(tpcwClient,
      numClients = loaderConfig.numLoaders,
      numEBs = loaderConfig.numEBs,
      numItems = loaderConfig.numItems)

    for(iteration <- (1 to iterations)) {
      logger.info("Begining iteration %d", iteration)
      TpcwScaleExperiment.results ++= (1 to threads).pmap(threadId => {
        def getTime = System.nanoTime / 1000000
        val histogram = Histogram(1, 5000)
        val runTime = runLengthMin * 60 * 1000L
        val iterationStartTime = getTime
        var endTime = iterationStartTime
        var skips = 0
        var failures = 0 // unused...

        val workflow = new TpcwWorkflow(loader)

        while(endTime - iterationStartTime < runTime) {
          val startTime = getTime
          try {
            val (axn, wasExecuted) = workflow.executeMix()
            endTime = getTime
            val elapsedTime = endTime - startTime
            if (wasExecuted) { // we actually ran the query
              histogram += elapsedTime
            } else // we punted the query
              skips += 1
          } catch {
            case e => {
              logger.warning(e, "Execepting generating page")
              skips += 1
            }
          }
        }

        logger.info("Thread %d stats 50th: %dms, 90th: %dms, 99th: %dms, avg: %fms, stddev: %fms",
            threadId, histogram.quantile(0.50), histogram.quantile(0.90), histogram.quantile(0.99), histogram.average, histogram.stddev)
        (ResultKey(this, loaderConfig, clusterRoot.canonicalAddress, clientId, iteration, threadId),
         ResultValue(endTime - iterationStartTime, histogram, skips))
      })

      coordination.registerAndAwait("iteration" + iteration, numClients)
    }

    //if(clientId == 0)
    //  cluster.shutdown

    System.exit(0)

  }
}
