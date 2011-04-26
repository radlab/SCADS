package edu.berkeley.cs
package scads
package piql
package tpcw
package scale

import comm._
import perf._
import storage._
import avro.runtime._
import avro.marker._

import deploylib._
import deploylib.mesos._

import scala.util.Random
import scala.collection.{ mutable => mu }

case class Result(var clientConfig: TpcwWorkflowTask,
                  var loaderConfig: TpcwLoaderTask,
                  var clusterAddress: String,
                  var clientId: Int,
                  var iteration: Int,
                  var threadId: Int) extends AvroPair {
  var totalElaspedTime: Long = _ /* in ms */
  var times: Histogram = null
  var failures: Int = _
}

case class TpcwWorkflowTask(var numClients: Int,
                            var executorClass: String,
                            var iterations: Int = 3,
                            var runLengthMin: Int = 5) extends AvroRecord with ReplicatedExperimentTask {

  var experimentAddress: String = _
  var clusterAddress: String = _
  var resultClusterAddress: String = _

  def run(): Unit = {

    val clientId = coordination.registerAndAwait("clientStart", numClients)

    logger.info("Waiting for cluster to be ready")
    val clusterConfig = clusterRoot.awaitChild("clusterReady")
    val loaderConfig = classOf[TpcwLoaderTask].newInstance.parse(clusterConfig.data)

    val results = resultCluster.getNamespace[Result]("tpcwScaleResults")
    val executor = Class.forName(executorClass).newInstance.asInstanceOf[QueryExecutor]
    val tpcwClient = new TpcwClient(cluster, executor)

    val threads = scala.math.ceil(loaderConfig.numEBs / numClients.toDouble).toInt
    logger.info("client going to use %d threads (1 EB per thread)", threads)

    val loader = new TpcwLoader(
      numEBs = loaderConfig.numEBs,
      numItems = loaderConfig.numItems)

    for(iteration <- (1 to iterations)) {
      logger.info("Begining iteration %d", iteration)
      results ++= (1 to threads).pmap(threadId => {
        def getTime = System.nanoTime / 1000000
        val histogram = Histogram(1, 5000)
        val runTime = runLengthMin * 60 * 1000L
        val iterationStartTime = getTime
        var endTime = iterationStartTime
        var skips = 0
        var failures = 0 // unused...

        val workflow = new TpcwWorkflow(tpcwClient, loader)

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
        val res = Result(this, loaderConfig, clusterRoot.canonicalAddress, clientId, iteration, threadId)
        res.totalElaspedTime =  endTime - iterationStartTime
        res.times = histogram
        res.failures = skips

        res
      })

      coordination.registerAndAwait("iteration" + iteration, numClients)
    }

    //if(clientId == 0)
    //  cluster.shutdown

    System.exit(0)

  }
}
