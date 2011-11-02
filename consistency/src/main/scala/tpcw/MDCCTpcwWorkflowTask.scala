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

case class MDCCResult(var clientConfig: MDCCTpcwWorkflowTask,
                      var loaderConfig: MDCCTpcwLoaderTask,
                      var clusterAddress: String,
                      var clientId: Int,
                      var iteration: Int,
                      var threadId: Int) extends AvroPair {
  var totalElaspedTime: Long = _ /* in ms */
  var times: Histogram = null
  var skips: Int = _
  var failures: Int = _
}

case class MDCCTpcwWorkflowTask(var numClients: Int,
                                var executorClass: String,
                                var numThreads: Int = 10,
                                var iterations: Int = 3,
                                var runLengthMin: Int = 5) extends AvroRecord with ReplicatedExperimentTask {

  var experimentAddress: String = _
  var clusterAddress: String = _
  var resultClusterAddress: String = _

  def run(): Unit = {

    val clientId = coordination.registerAndAwait("clientStart", numClients, timeout=60*60*1000)

    logger.info("Waiting for cluster to be ready")
    val clusterConfig = clusterRoot.awaitChild("clusterReady")
    val loaderConfig = classOf[MDCCTpcwLoaderTask].newInstance.parse(clusterConfig.data)

    val results = resultCluster.getNamespace[MDCCResult]("tpcwMDCCResults")
    val executor = Class.forName(executorClass).newInstance.asInstanceOf[QueryExecutor]
    val tpcwClient = new MDCCTpcwClient(cluster, executor, loaderConfig.txProtocol)

    val loader = new TpcwLoader(
      numEBs = loaderConfig.numEBs,
      numItems = loaderConfig.numItems)

    for(iteration <- (1 to iterations)) {
      logger.info("Begining iteration %d", iteration)
      results ++= (1 to numThreads).pmap(threadId => {
        def getTime = System.nanoTime / 1000000
        val histogram = Histogram(1, 5000)
        val runTime = runLengthMin * 60 * 1000L
        val iterationStartTime = getTime
        var endTime = iterationStartTime
        var skips = 0
        var failures = 0

        val workflow = new TpcwWorkflow(tpcwClient, loader)

        while(endTime - iterationStartTime < runTime) {
          val startTime = getTime
          try {
            val wasExecuted = workflow.executeMix()
            endTime = getTime
            val elapsedTime = endTime - startTime
            if (wasExecuted) { // we actually ran the query
              histogram += elapsedTime
            } else // we punted the query
              skips += 1
          } catch {
            case e => {
              logger.warning(e, "Execepting generating page")
              failures += 1
            }
          }
        }

        logger.info("Thread %d stats 50th: %dms, 90th: %dms, 99th: %dms, avg: %fms, stddev: %fms",
            threadId, histogram.quantile(0.50), histogram.quantile(0.90), histogram.quantile(0.99), histogram.average, histogram.stddev)
        val res = MDCCResult(this, loaderConfig, clusterRoot.canonicalAddress, clientId, iteration, threadId)
        res.totalElaspedTime =  endTime - iterationStartTime
        res.times = histogram
        res.failures = failures
        res.skips = skips

        res
      })

      coordination.registerAndAwait("iteration" + iteration, numClients)
    }

    if(clientId == 0) {
      ExperimentNotification.completions.publish("TPCW MDCC Complete", this.toJson)
      cluster.shutdown
    }
  }
}
