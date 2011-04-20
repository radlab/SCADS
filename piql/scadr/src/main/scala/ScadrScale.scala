package edu.berkeley.cs
package scads
package perf
package scadr
package scale

import comm._
import piql._
import piql.scadr._
import storage._
import avro.runtime._
import avro.marker._

import deploylib._
import deploylib.mesos._
import deploylib.ec2._

import scala.util.Random

case class Result(var clientConfig: ScadrScaleTask,
                  var loaderConfig: ScadrLoaderTask,
                  var clusterAddress: String,
                  var clientId: Int,
                  var iteration: Int,
                  var threadId: Int) extends AvroPair {
  var times: Histogram = null
  var skips: Int = 0
}

case class ScadrScaleTask(var numClients: Int,
                          var executorClass: String,
                          var writeProbability: Double,
                          var iterations: Int = 5,
                          var threads: Int = 50,
                          var runLengthMin: Int = 5,
                          var resultsPerPage: Int = 10) extends AvroRecord with ReplicatedExperimentTask {
  var clusterAddress: String = _
  var resultClusterAddress: String = _
  var experimentAddress: String = _

  /* True if coin flip with prob succeeds */
  protected def flipCoin(prob: Double): Boolean = Random.nextDouble < prob

  def run() = {
    val results = new ScadsCluster(ZooKeeperNode(resultClusterAddress)).getNamespace[Result]("scadrScaleResults")
    val clusterRoot = ZooKeeperNode(clusterAddress)
    val coordination = clusterRoot.getOrCreate("coordination/clients")
    val cluster = new ScadsCluster(clusterRoot)
    var executor = Class.forName(executorClass).newInstance.asInstanceOf[QueryExecutor]
    val clientId = coordination.registerAndAwait("clientStart", numClients)

    logger.info("Waiting for cluster to be ready")
    val clusterConfig = clusterRoot.awaitChild("clusterReady")
    val loaderConfig = classOf[ScadrLoaderTask].newInstance.parse(clusterConfig.data)
    val scadrClient = new ScadrClient(cluster, executor)


    //TODO: Seperate ScadrData and ScadrLoader, move this to a function
    val loader = new ScadrLoader(
      replicationFactor = loaderConfig.replicationFactor,
      numClients = loaderConfig.numLoaders,
      numUsers = loaderConfig.numServers * 10000 / loaderConfig.replicationFactor,
      numThoughtsPerUser = 100,
      numSubscriptionsPerUser = loaderConfig.followingCardinality,
      numTagsPerThought = 5)

    for (iteration <- (1 to iterations)) {
      logger.info("Begining iteration %d", iteration)

      results ++= (1 to threads).pmap(threadId => {
        def getTime = System.nanoTime / 1000000
        val result = Result(this, loaderConfig, clusterRoot.canonicalAddress, clientId, iteration, threadId)
        result.times = Histogram(1, 1000)
        val runTime = runLengthMin * 60 * 1000L
        val iterationStartTime = getTime
        var endTime = iterationStartTime
        var skips = 0
        var failures = 0

        while (endTime - iterationStartTime < runTime) {
          val startTime = getTime

          try {
            // Here we try to emulate a page load for scadr
            // the assertions below are to prevent the compiler/jvm from
            // optimizing away the queries

            val currentUser = loader.randomUser

            // 1) load the user's thoughtstream
            val ts = scadrClient.thoughtstream(currentUser, resultsPerPage)
            assert(ts != null)

            // 2) load the user's followers
            val followers = scadrClient.usersFollowing(currentUser, resultsPerPage)
            assert(followers != null)

            // 3) load the user's followings
            val followings = scadrClient.usersFollowedBy(currentUser, resultsPerPage)
            assert(followings != null)

            // 4) make a tweet with some probability
            if (flipCoin(writeProbability)) {
              val thoughtTime = getTime.toInt
              val thought = new Thought(currentUser, thoughtTime)
              thought.text = "New thought by user %s at time %d".format(currentUser, thoughtTime)
              scadrClient.thoughts.put(thought.key, thought.value)
            }

            endTime = getTime
            val elapsedTime = endTime - startTime
            result.times += elapsedTime
          }
          catch {
            case e => {
              logger.warning(e, "Execepting generating page")
              skips += 1
            }
          }
        }

        logger.info("Thread %d stats 50th: %dms, 90th: %dms, 99th: %dms", threadId, result.times.quantile(0.50), result.times.quantile(0.90), result.times.quantile(0.99))

        result
      })

      coordination.registerAndAwait("iteration" + iteration, numClients)
    }

    if (clientId == 0) {
      ExperimentNotification.completions.publish("Scadr Scale Experiment Complete", this.toJson)
      cluster.shutdown
    }
  }
}
