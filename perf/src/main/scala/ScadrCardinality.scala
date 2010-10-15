package edu.berkeley.cs
package scads
package perf

import comm._
import piql._
import storage._
import avro.runtime._
import avro.marker._

import deploylib._
import deploylib.mesos._

case class ScadrCardinalityClient(var cardinality: Int, var cluster: String, var clientId: Int, var iteration: Int, var threadId: Int) extends AvroRecord
case class ScadrCardinalityResult(var times: Histogram, var skips: Int) extends AvroRecord

object ScadrCardinalityTest extends Experiment {
  val results = resultCluster.getNamespace[ScadrCardinalityClient, ScadrCardinalityResult]("scadrCardinality")

  def clear = results.getRange(None, None).foreach(r => results.put(r._1, None))

  def printResults: Unit = {
    val runs = results.getRange(None, None).groupBy(k => (k._1.cluster, k._1.iteration)).filterNot(_._1._2 == 1).values
    runs.foreach(run => {
      val totalRequests = run.map(_._2.times.buckets.sum).sum
      val aggregrateHistogram = run.map(_._2.times).reduceLeft(_ + _)
      val cumulativeHistogram = aggregrateHistogram.buckets.scanLeft(0L)(_ + _).drop(1)
      val quantile50ResponseTime = cumulativeHistogram.findIndexOf(_ >= totalRequests * 0.50) * aggregrateHistogram.bucketSize
      val quantile99ResponseTime = cumulativeHistogram.findIndexOf(_ >= totalRequests * 0.99) * aggregrateHistogram.bucketSize
      val quantile999ResponseTime = cumulativeHistogram.findIndexOf(_ >= totalRequests * 0.999) * aggregrateHistogram.bucketSize

      println(List(run.head._1.cardinality, totalRequests, quantile50ResponseTime, quantile99ResponseTime, quantile999ResponseTime).mkString("\t"))
    })
  }

  def run(followingCardinality: Int, clusterSize: Int = 10)(implicit classpath: Seq[ClassSource], scheduler: ExperimentScheduler): ZooKeeperProxy#ZooKeeperNode = {
    val expRoot = newExperimentRoot

    scheduler.scheduleExperiment(
      serverJvmProcess(expRoot.canonicalAddress) * clusterSize ++
      clientJvmProcess(
        LoadClient(
          expRoot.canonicalAddress,
          clusterSize,
          clusterSize,
          followingCardinality
        )
      ) * clusterSize
    )

    expRoot
  }

  case class LoadClient(var clusterAddress: String, var numServers: Int, var numClients: Int, var followingCardinality: Int, var iterations: Int = 5, var threads: Int = 50, var runLengthMin: Int = 5 ) extends AvroRecord with Runnable {
    def run() = {
      val clusterRoot = ZooKeeperNode(clusterAddress)
      val coordination = clusterRoot.getOrCreate("coordination")
      val cluster = new ScadsCluster(clusterRoot)
      val scadrClient = new ScadrClient(cluster, new SimpleExecutor)
      val loader = new ScadrLoader(scadrClient, numClients,
        numUsers = numServers * 10000,
        numThoughtsPerUser = 100,
        numSubscriptionsPerUser = followingCardinality,
        numTagsPerThought = 5)

      val clientId = coordination.registerAndAwait("clientStart", numClients)
      if(clientId == 0) {
        logger.info("Awaiting scads cluster startup")
        cluster.blockUntilReady(numServers)
        loader.createNamespaces
      }

      coordination.registerAndAwait("startBulkLoad", numClients)
      logger.info("Begining bulk loading of data")
      loader.getData(clientId).load()
      logger.info("Bulk loading complete")
      coordination.registerAndAwait("loadingComplete", numClients)

      for(iteration <- (1 to iterations)) {
        logger.info("Begining iteration %d", iteration)

        results ++= (1 to threads).pmap(threadId => {
          def getTime = System.currentTimeMillis
          val histogram = Histogram(1, 5000)
          val runTime = runLengthMin * 60 * 1000L
          val iterationStartTime = getTime
          var endTime = iterationStartTime
          var skips = 0
          var failures = 0

          while(endTime - iterationStartTime < runTime) {
            val startTime = getTime
            scadrClient.thoughtstream(loader.randomUser, scadrClient.maxResultsPerPage)
            endTime = getTime
            val elapsedTime = endTime - startTime
            if(elapsedTime < 0) {
              logger.warning("Time Skip: %d", elapsedTime)
              skips += 1
            }
            else {
              histogram.add(endTime - startTime)
            }
          }

          logger.info("Thread %d complete", threadId)
          (ScadrCardinalityClient(followingCardinality, clusterAddress, clientId, iteration, threadId), ScadrCardinalityResult(histogram, skips))
        })

        coordination.registerAndAwait("iteration" + iteration, numClients)
      }

      if(clientId == 0)
        cluster.shutdown

      System.exit(0)
    }
  }
}
