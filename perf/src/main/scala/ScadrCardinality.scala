package edu.berkeley.cs
package scads
package perf
package scadr.cardinality

import comm._
import piql._
import storage._
import avro.runtime._
import avro.marker._

import deploylib._
import deploylib.mesos._

import java.io.File

case class ResultKey(var clientConfig: ThoughtStreamClient, var loaderConfig: ScadrLoaderClient, var clusterAddress: String, var clientId: Int, var iteration: Int, var threadId: Int) extends AvroRecord
case class ResultValue(var times: Histogram, var failures: Int) extends AvroRecord
case class Result(var key: ResultKey, var values: ResultValue) extends AvroRecord

object CardinalityExperiment extends Experiment {
  lazy val results = resultCluster.getNamespace[ResultKey, ResultValue]("scadrCardinality")

  def allResults = results.getRange(None,None)

  def backupResults = {
    val outfile = AvroOutFile[Result](new File("cardinalityResults" + System.currentTimeMillis + ".avro"))
    allResults.map(r => Result(r._1, r._2)).foreach(outfile.append)
    outfile.close
  }

  def clear = results.getRange(None, None).foreach(r => results.put(r._1, None))

  def makeGraph(implicit classpath: Seq[ClassSource], scheduler: ExperimentScheduler, zookeeper: ZooKeeperProxy#ZooKeeperNode) = {
    val expSize = 1
    (100 to 1000 by 100).foreach(cardinality => {
      val executors = List("Simple", "Parallel", "BulkParallel").map(e => "edu.berkeley.cs.scads.piql.%sExecutor".format(e))
      throw new RuntimeException("Broken")
    })
  }

  def newCluster(loaderDesc: ScadrLoaderClient)(implicit classpath: Seq[ClassSource], scheduler: ExperimentScheduler, zookeeper: ZooKeeperProxy#ZooKeeperNode): ScadsCluster = {
    val clusterRoot = newExperimentRoot
    scheduler.scheduleExperiment(serverJvmProcess(clusterRoot.canonicalAddress) * loaderDesc.numServers ++ clientJvmProcess(loaderDesc, clusterRoot) * loaderDesc.numLoaders)
    new ScadsCluster(clusterRoot)
  }

  def run(clientDesc: ThoughtStreamClient, cluster: ScadsCluster)(implicit classpath: Seq[ClassSource], scheduler: ExperimentScheduler): Unit = {
    cluster.root("coordination").get("clients").foreach(_.deleteRecursive)
    scheduler.scheduleExperiment(clientJvmProcess(clientDesc, cluster.root) * clientDesc.numClients)
  }

  def printResults: Unit = {
    val runs = results.getRange(None, None).groupBy(k => (k._1.clientConfig, k._1.iteration)).filterNot(_._1._2 == 1).values
    runs.foreach(run => {
      val totalRequests = run.map(_._2.times.buckets.sum).sum
      val aggregrateHistogram = run.map(_._2.times).reduceLeft(_ + _)
      val cumulativeHistogram = aggregrateHistogram.buckets.scanLeft(0L)(_ + _).drop(1)
      val quantile50ResponseTime = cumulativeHistogram.findIndexOf(_ >= totalRequests * 0.50) * aggregrateHistogram.bucketSize
      val quantile90ResponseTime = cumulativeHistogram.findIndexOf(_ >= totalRequests * 0.90) * aggregrateHistogram.bucketSize
      val quantile99ResponseTime = cumulativeHistogram.findIndexOf(_ >= totalRequests * 0.99) * aggregrateHistogram.bucketSize
      val quantile999ResponseTime = cumulativeHistogram.findIndexOf(_ >= totalRequests * 0.999) * aggregrateHistogram.bucketSize
      val failures = run.map(_._2.failures).sum

      println(List(run.head._1.loaderConfig.followingCardinality,
        failures,
        run.head._1.clientConfig.executorClass,
        totalRequests,
        aggregrateHistogram.quantile(0.5),
        aggregrateHistogram.quantile(0.90),
        aggregrateHistogram.quantile(0.99),
        aggregrateHistogram.quantile(0.999)).mkString("\t"))
    })
  }

}

case class ScadrLoaderClient(var numServers: Int, var numLoaders: Int, var followingCardinality: Int, var replicationFactor: Int = 1) extends AvroClient with AvroRecord {
  def run(clusterRoot: ZooKeeperProxy#ZooKeeperNode) = {
    val coordination = clusterRoot.getOrCreate("coordination/loaders")
    val cluster = new ExperimentalScadsCluster(clusterRoot)
    val scadrClient = new ScadrClient(cluster, new SimpleExecutor)
    val loader = new ScadrLoader(scadrClient,
      replicationFactor = replicationFactor,
      numClients = numLoaders,
      numUsers = numServers * 10000 / replicationFactor,
      numThoughtsPerUser = 100,
      numSubscriptionsPerUser = followingCardinality,
      numTagsPerThought = 5)

    val clientId = coordination.registerAndAwait("clientStart", numLoaders)
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

    coordination.registerAndAwait("startBulkLoad", numLoaders)
    logger.info("Begining bulk loading of data")
    loader.getData(clientId).load()
    logger.info("Bulk loading complete")
    coordination.registerAndAwait("loadingComplete", numLoaders)

    if(clientId == 0)
      clusterRoot.createChild("clusterReady", data=this.toBytes)

    System.exit(0)
  }
}

case class ThoughtStreamClient(var numClients: Int, var executorClass: String, var iterations: Int = 5, var threads: Int = 1, var runLengthMin: Int = 5) extends AvroClient with AvroRecord {
  def run(clusterRoot: ZooKeeperProxy#ZooKeeperNode): Unit = {
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

      CardinalityExperiment.results ++= (1 to threads).pmap(threadId => {
        def getTime = System.nanoTime / 1000000
        val histogram = Histogram(1, 5000)
        val runTime = runLengthMin * 60 * 1000L
        val iterationStartTime = getTime
        var endTime = iterationStartTime
        var failures = 0

        while(endTime - iterationStartTime < runTime) {
          val startTime = getTime
          try {
            scadrClient.thoughtstream(loader.randomUser, scadrClient.maxResultsPerPage)
            endTime = getTime
            val elapsedTime = endTime - startTime
            histogram.add(endTime - startTime)
          }
          catch {
            case e => {
              logger.warning(e, "Query Failed")
              failures += 1
              Thread.sleep(100)
            }
          }
        }

        logger.info("Thread %d stats 50th: %dms, 90th: %dms, 99th: %dms", threadId, histogram.quantile(0.50), histogram.quantile(0.90), histogram.quantile(0.99))
        (ResultKey(this, loaderConfig, clusterRoot.canonicalAddress, clientId, iteration, threadId), ResultValue(histogram, failures))
      })

      coordination.registerAndAwait("iteration" + iteration, numClients)
    }

    if(clientId == 0)
      cluster.shutdown

    System.exit(0)
  }
}
