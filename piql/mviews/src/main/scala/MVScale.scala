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
case class ScaleTask(var replicas: Int = 2,
                     var partitions: Int = 2,
                     var nClients: Int = 1,
                     var iterations: Int = 2,
                     var itemsPerMachine: Seq[Int] = List(1000,10000),
                     var maxTagsPerItem: Int = 10,
                     var meanTagsPerItem: Int = 4,
                     var threadCounts: Seq[Int] = Seq(1,8,32))
            extends AvroTask with AvroRecord with TaskBase {
  
  var resultClusterAddress: String = _
  var clusterAddress: String = _

  def schedule(resultClusterAddress: String)(implicit cluster: deploylib.mesos.Cluster,
                                             classSource: Seq[ClassSource]): Unit = {
    val scadsCluster = newScadsCluster(replicas * partitions)
    clusterAddress = scadsCluster.root.canonicalAddress
    this.resultClusterAddress = resultClusterAddress
    val task = this.toJvmTask
    cluster.serviceScheduler.scheduleExperiment(task :: Nil)
  }

  def setupPartitions(client: MVTest, cluster: ScadsCluster) = {
    val numServers: Int = replicas * partitions
    val available = cluster.getAvailableServers
    assert (available.size >= numServers)

    val groups = available.take(numServers).grouped(replicas).toSeq
    var p: List[(Option[Array[Byte]], Seq[StorageService])] = List()
    for (servers <- groups) {
      if (p.length == 0) {
        p ::= (None, servers)
      } else {
        p ::= (client.indexKeyspace(p.length, partitions), servers)
      }
    }
    p = p.reverse
    logger.info("Partition scheme: " + p)

    val nn = List(
      cluster.getNamespace[Tag]("tags"),
      cluster.getNamespace[MTagPair]("mTagPairs"))
    for (n <- nn) {
      n.setPartitionScheme(p)
      n.setReadWriteQuorum(.001, 1.00)
    }
  }

  def run(): Unit = {
    val logger = Logger()
    val cluster = new ExperimentalScadsCluster(ZooKeeperNode(clusterAddress))
    cluster.blockUntilReady(replicas * partitions)

    val resultCluster = new ScadsCluster(ZooKeeperNode(resultClusterAddress))
    val results = resultCluster.getNamespace[ParResult]("ParResult")

    val hostname = java.net.InetAddress.getLocalHost.getHostName

    val coordination = cluster.root.getOrCreate("coordination/loaders")
    val clientNumber = coordination.registerAndAwait("clientsStart", nClients)

    // setup client (AFTER namespace creation)
    val clients =
      List(/*new NaiveTagClient(cluster, new ParallelExecutor),*/
           new MTagClient(cluster, new ParallelExecutor))

    clients.foreach(client => {
      val clientId = client.getClass.getSimpleName
      val scenario = new MVTest(cluster, client)
      itemsPerMachine.foreach(ii => {
        val loadStartMs = System.currentTimeMillis

        /* single client loads all data */
        if (clientNumber == 0) {
          logger.info("Client %d preparing data...", clientNumber)
          scenario.reset()
          setupPartitions(scenario, cluster)
          scenario.randomPopulate(ii * partitions,
            meanTagsPerItem, maxTagsPerItem)
        } else {
          logger.info("Client %d awaiting dataReady", clientNumber)
        }
        val loadTimeMs = System.currentTimeMillis - loadStartMs
        logger.info("Data load wait: %d ms", loadTimeMs)

        coordination.registerAndAwait("dataReady" + ii, nClients)
        (1 to iterations).foreach(iteration => {
          logger.info("Beginning iteration %d", iteration)
          threadCounts.foreach(threadCount => {
            val iterationStartMs = System.currentTimeMillis
            val failures = new java.util.concurrent.atomic.AtomicInteger()
            val histograms = (0 until threadCount).pmap(i => {
              val histogram = Histogram(100,10000)
              var i = 20000
              while (i > 0) {
                i -= 1
                try {
                  val respTime = scenario.randomAction
                  logger.debug("Get response time: %d", respTime)
                  histogram.add(respTime)
                } catch {
                  case e => 
                    failures.getAndAdd(1)
                }
              }
              histogram
            })

            val r = ParResult(System.currentTimeMillis, hostname, iteration, clientId)
            r.threadCount = threadCount
            r.clientNumber = clientNumber
            r.nClients = nClients
            r.replicas = replicas
            r.partitions = partitions
            r.itemsPerMachine = ii
            r.maxTags = maxTagsPerItem
            r.meanTags = meanTagsPerItem
            r.loadTimeMs = loadTimeMs
            r.runTimeMs = System.currentTimeMillis - iterationStartMs
            r.responseTimes = histograms.reduceLeft(_ + _)
            r.failures = failures.get()
            results.put(r)
          })
        })
      })
    })
  }
}
