package edu.berkeley.cs
package scads
package perf
package scadr
package scale

import comm._
import piql._
import storage._
import avro.runtime._
import avro.marker._

import deploylib._
import deploylib.mesos._

import scala.util.Random

case class ResultKey(var clientConfig: LoadClient, var loaderConfig: ScadrLoaderClient, var clusterAddress: String, var clientId: Int, var iteration: Int, var threadId: Int) extends AvroRecord
case class ResultValue(var times: Histogram, var skips: Int) extends AvroRecord

object ScadrScaleExperiment extends Experiment {
  val results = resultCluster.getNamespace[ResultKey, ResultValue]("scadrScale")

  def clear = results.getRange(None, None).foreach(r => results.put(r._1, None))

  def printResults: Unit = {
    val runs = results.getRange(None, None).groupBy(k => (k._1.clientConfig, k._1.iteration)).filterNot(_._1._2 == 1).values
    println(List("numServers", "execClass", "totalReqs", "50thPercentile", "99thPercentile", "99.9thPercentile").mkString("\t"))
    runs.foreach(run => {
      val totalRequests = run.map(_._2.times.buckets.sum).sum
      val aggregrateHistogram = run.map(_._2.times).reduceLeft(_ + _)
      val cumulativeHistogram = aggregrateHistogram.buckets.scanLeft(0L)(_ + _).drop(1)
      val quantile50ResponseTime = cumulativeHistogram.findIndexOf(_ >= totalRequests * 0.50) * aggregrateHistogram.bucketSize
      val quantile99ResponseTime = cumulativeHistogram.findIndexOf(_ >= totalRequests * 0.99) * aggregrateHistogram.bucketSize
      val quantile999ResponseTime = cumulativeHistogram.findIndexOf(_ >= totalRequests * 0.999) * aggregrateHistogram.bucketSize

      println(List(run.head._1.loaderConfig.numServers, run.head._1.clientConfig.executorClass, totalRequests, quantile50ResponseTime, quantile99ResponseTime, quantile999ResponseTime).mkString("\t"))
    })
  }

  def makeGraphPoint(size: Int)(implicit classpath: Seq[ClassSource], scheduler: ExperimentScheduler, zookeeper: ZooKeeperProxy#ZooKeeperNode): Unit = {
    val cluster = ScadrLoaderClient(size, size, 10).newCluster(size, size)
    LoadClient(size, "edu.berkeley.cs.scads.piql.SimpleExecutor", 0.01, threads=10).schedule(cluster)
  }
}

case class LoadClient(var numClients: Int, var executorClass: String, var writeProbability: Double, var iterations: Int = 5, var threads: Int = 50, var runLengthMin: Int = 5) extends AvroRecord with ReplicatedAvroClient {
  /* True if coin flip with prob succeeds */
  protected def flipCoin(prob: Double): Boolean = Random.nextDouble < prob

  def run(clusterRoot: ZooKeeperProxy#ZooKeeperNode) = {
    val coordination = clusterRoot.getOrCreate("coordination/clients")
    val cluster = new ScadsCluster(clusterRoot)
    var executor = Class.forName(executorClass).newInstance.asInstanceOf[QueryExecutor]
    val scadrClient = new ScadrClient(cluster, executor)

    val clientId = coordination.registerAndAwait("clientStart", numClients)

    logger.info("Waiting for cluster to be ready")
    val clusterConfig = clusterRoot.awaitChild("clusterReady")
    val loaderConfig = classOf[ScadrLoaderClient].newInstance.parse(clusterConfig.data)

    //TODO: Seperate ScadrData and ScadrLoader, move this to a function
    val loader = new ScadrLoader(scadrClient,
      replicationFactor = loaderConfig.replicationFactor,
      numClients = loaderConfig.numLoaders,
      numUsers = loaderConfig.numServers * 10000 / loaderConfig.replicationFactor,
      numThoughtsPerUser = 100,
      numSubscriptionsPerUser = loaderConfig.followingCardinality,
      numTagsPerThought = 5)

    for(iteration <- (1 to iterations)) {
      logger.info("Begining iteration %d", iteration)

      ScadrScaleExperiment.results ++= (1 to threads).pmap(threadId => {
        def getTime = System.nanoTime / 1000000
        val histogram = Histogram(1, 5000)
        val runTime = runLengthMin * 60 * 1000L
        val iterationStartTime = getTime
        var endTime = iterationStartTime
        var skips = 0
        var failures = 0

        while(endTime - iterationStartTime < runTime) {
          val startTime = getTime

          try {
            // Here we try to emulate a page load for scadr
            // the assertions below are to prevent the compiler/jvm from
            // optimizing away the queries

            val currentUser = loader.randomUser

            // 1) load the user's thoughtstream
            val ts = scadrClient.thoughtstream(currentUser, scadrClient.maxResultsPerPage)
            assert(ts != null)

            // 2) load the user's followers
            val followers = scadrClient.usersFollowing(currentUser, scadrClient.maxResultsPerPage)
            assert(followers != null)

            // 3) load the user's followings
            val followings = scadrClient.usersFollowedBy(currentUser, scadrClient.maxResultsPerPage)
            assert(followings != null)

            // 4) make a tweet with some probability
            if (flipCoin(writeProbability)) {
              val thoughtTime = getTime.toInt
              scadrClient.saveThought(
                  ThoughtKey(currentUser, thoughtTime),
                  ThoughtValue("New thought by user %s at time %d".format(currentUser, thoughtTime)))
            }

            endTime = getTime
            val elapsedTime = endTime - startTime
            histogram += elapsedTime
          }
          catch {
            case e => {
              logger.warning(e, "Execepting generating page")
              skips += 1
            }
          }
        }

        logger.info("Thread %d stats 50th: %dms, 90th: %dms, 99th: %dms", threadId, histogram.quantile(0.50), histogram.quantile(0.90), histogram.quantile(0.99))
        (ResultKey(this, loaderConfig, clusterRoot.canonicalAddress, clientId, iteration, threadId), ResultValue(histogram, skips))
      })

      coordination.registerAndAwait("iteration" + iteration, numClients)
    }

    if(clientId == 0)
      cluster.shutdown

    System.exit(0)

  }
}
