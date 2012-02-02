package edu.berkeley.cs
package scads
package piql
package mviews

import deploylib._
import deploylib.mesos._
import avro.marker._
import perf._
import comm._
import config._
import storage._
import exec._

import net.lag.logging.Logger

/* task to run MVTest on EC2 */
case class Task(var replicationFactor: Int = 2,
                var iterations: Int = 2,
                var scales: Seq[Int] = List(10,100,500,1000,5000,10000),
                var threadCounts: Seq[Int] = Seq(1))
            extends AvroTask with AvroRecord with TaskBase {
  
  var resultClusterAddress: String = _
  var clusterAddress: String = _

  def schedule(resultClusterAddress: String)(implicit cluster: deploylib.mesos.Cluster,
                                             classSource: Seq[ClassSource]): Unit = {
    val scadsCluster = newScadsCluster(replicationFactor)
    clusterAddress = scadsCluster.root.canonicalAddress
    this.resultClusterAddress = resultClusterAddress
    val task = this.toJvmTask
    cluster.serviceScheduler.scheduleExperiment(task :: Nil)
  }

  def setupPartitions(cluster: ScadsCluster, replicationFactor: Int) = {
    val partitions = List((None, cluster.getAvailableServers.take(replicationFactor)))
    val nn = List(
      cluster.getNamespace[Tag]("tags"),
      cluster.getNamespace[MTagPair]("mTagPairs"))
    for (n <- nn) {
      n.setPartitionScheme(partitions)
      n.setReadWriteQuorum(.001, 1.00)
    }
  }

  def run(): Unit = {
    val logger = Logger()
    val cluster = new ExperimentalScadsCluster(ZooKeeperNode(clusterAddress))
    cluster.blockUntilReady(replicationFactor)

    val resultCluster = new ScadsCluster(ZooKeeperNode(resultClusterAddress))
    val results = resultCluster.getNamespace[MVResult](MVResult.suffix)

    val hostname = java.net.InetAddress.getLocalHost.getHostName

    setupPartitions(cluster, replicationFactor)

    // setup client (AFTER namespace creation)
    val clients =
      List(new NaiveTagClient(cluster, new ParallelExecutor),
           new MTagClient(cluster, new ParallelExecutor))

    clients.foreach(client => {
      val clientId = client.getClass.getSimpleName
      val scenario = new MVTest(cluster, client)
      scales.foreach(scale => {
        scenario.reset()
        val loadStartMs = System.currentTimeMillis
        scenario.pessimalScaleup(scale)
        val loadTimeMs = System.currentTimeMillis - loadStartMs
        logger.info("Data load: %d ms", loadTimeMs)

        (1 to iterations).foreach(iteration => {
          logger.info("Beginning iteration %d", iteration)
          threadCounts.foreach(threadCount => {
            val iterationStartMs = System.currentTimeMillis
            val failures = new java.util.concurrent.atomic.AtomicInteger()
            val histograms = (0 until threadCount).pmap(i => {
              val histogram = Histogram(100,10000)
              var i = 1000000 / scale
              if (clientId equals "MTagClient")
                i = 100000
              while (i > 0) {
                i -= 1
                try {
                  val respTime = scenario.doPessimalFetch
                  logger.debug("Get response time: %d", respTime)
                  histogram.add(respTime)
                } catch {
                  case e => 
                    failures.getAndAdd(1)
                }
              }
              histogram
            })

            val r = MVResult(hostname, clientId, iteration, scale, threadCount)
            r.timestamp = System.currentTimeMillis
            r.failures = failures.get()
            r.responseTimes = histograms.reduceLeft(_ + _)
            r.loadTimeMs = loadTimeMs
            r.runTimeMs = System.currentTimeMillis - iterationStartMs
            results.put(r)
          })
        })
      })
    })
  }
}
