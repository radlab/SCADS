package edu.berkeley.cs
package scads
package perf
package scadr.scale

import comm._
import piql._
import storage._
import avro.runtime._
import avro.marker._

import deploylib._
import deploylib.mesos._

import scala.util.Random

case class ResultKey(var clientConfig: LoadClient, var clusterAddress: String, var clientId: Int, var iteration: Int, var threadId: Int) extends AvroRecord
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

      println(List(run.head._1.clientConfig.numServers, run.head._1.clientConfig.executorClass, totalRequests, quantile50ResponseTime, quantile99ResponseTime, quantile999ResponseTime).mkString("\t"))
    })
  }

  def makeGraph(implicit classpath: Seq[ClassSource], scheduler: ExperimentScheduler) =
    (10 to 100 by 10).foreach(n => run(LoadClient(n, n, 100, "edu.berkeley.cs.scads.piql.SimpleExecutor", 0.01)))

  def run(clientConfig: LoadClient)(implicit classpath: Seq[ClassSource], scheduler: ExperimentScheduler): ZooKeeperProxy#ZooKeeperNode = {
    val expRoot = newExperimentRoot
    val procs = serverJvmProcess(expRoot.canonicalAddress) * clientConfig.numServers ++ clientJvmProcess(clientConfig, expRoot) * clientConfig.numClients
    scheduler.scheduleExperiment(procs)
    expRoot
  }
}

case class LoadClient(var numServers: Int, var numClients: Int, var followingCardinality: Int, var executorClass: String, var writeProbability: Double, var iterations: Int = 5, var threads: Int = 50, var runLengthMin: Int = 5 ) extends AvroRecord with AvroClient {
  def run(clusterRoot: ZooKeeperProxy#ZooKeeperNode) = {
    //TODO: Can we mark vars as transient? so this can be outside of the function
    val random = new Random
    /* True if coin flip with prob succeeds */
    def flipCoin(prob: Double): Boolean = random.nextDouble < prob

    val coordination = clusterRoot.getOrCreate("coordination")
    val cluster = new ScadsCluster(clusterRoot)
    var executor = Class.forName(executorClass).newInstance.asInstanceOf[QueryExecutor]
    val scadrClient = new ScadrClient(cluster, executor)

    // TODO: configure the loader
    val loader = new ScadrLoader(scadrClient,
      replicationFactor = 1,
      numClients = numClients,
      numUsers = numServers * 1000,
      numThoughtsPerUser = 100,
      numSubscriptionsPerUser = followingCardinality,
      numTagsPerThought = 5)

    val clientId = coordination.registerAndAwait("clientStart", numClients)
    if(clientId == 0) {
      logger.info("Awaiting scads cluster startup")
      cluster.blockUntilReady(numServers)
      loader.createNamespaces
      scadrClient.users.setReadWriteQuorum(0.33, 0.67)
      scadrClient.thoughts.setReadWriteQuorum(0.33, 0.67)
      scadrClient.subscriptions.setReadWriteQuorum(0.33, 0.67)
      scadrClient.tags.setReadWriteQuorum(0.33, 0.67)
      scadrClient.idxUsersTarget.setReadWriteQuorum(0.33, 0.67)
    }

    coordination.registerAndAwait("startBulkLoad", numClients)
    logger.info("Begining bulk loading of data")
    loader.getData(clientId).load()
    logger.info("Bulk loading complete")
    coordination.registerAndAwait("loadingComplete", numClients)

    for(iteration <- (1 to iterations)) {
      logger.info("Begining iteration %d", iteration)

      ScadrScaleTest.results ++= (1 to threads).pmap(threadId => {
        def getTime = System.currentTimeMillis
        val histogram = Histogram(1, 5000)
        val runTime = runLengthMin * 60 * 1000L
        val iterationStartTime = getTime
        var endTime = iterationStartTime
        var skips = 0
        var failures = 0

        while(endTime - iterationStartTime < runTime) {
          val startTime = getTime

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
          if (elapsedTime < 0) {
            logger.warning("Time Skip: %d", elapsedTime)
            skips += 1
          } else {
            histogram += elapsedTime
          }
        }

        logger.info("Thread %d complete", threadId)
        (ResultKey(this, clusterRoot.canonicalAddress, clientId, iteration, threadId), ResultValue(histogram, skips))
      })

      coordination.registerAndAwait("iteration" + iteration, numClients)
    }

    if(clientId == 0)
      cluster.shutdown

    System.exit(0)

  }
}
