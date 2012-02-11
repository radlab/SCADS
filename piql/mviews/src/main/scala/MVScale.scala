package edu.berkeley.cs
package scads
package piql
package mviews

import scala.util.Random

import deploylib._
import deploylib.mesos._
import avro.marker._
import perf._
import comm._
import config._
import storage._
import exec._
import storage.client.index._

import net.lag.logging.Logger

/* task to run MVScaleTest on EC2 */
case class ScaleTask(var replicas: Int = 1,
                     var partitions: Int = 8,
                     var nClients: Int = 8,
                     var iterations: Int = 2,
                     var itemsPerMachine: Int = 50000,
                     var maxTagsPerItem: Int = 20,
                     var meanTagsPerItem: Int = 4,
                     var readFrac: Double = 0.8,
                     var threadCounts: Seq[Int] = Seq(32))
            extends AvroTask with AvroRecord with TaskBase {
  
  var resultClusterAddress: String = _
  var clusterAddress: String = _

  def schedule(resultClusterAddress: String)(implicit cluster: deploylib.mesos.Cluster,
                                             classSource: Seq[ClassSource]): Unit = {
    val scadsCluster = newScadsCluster(replicas * partitions)
    clusterAddress = scadsCluster.root.canonicalAddress
    this.resultClusterAddress = resultClusterAddress
    val task = this.toJvmTask
    (1 to nClients).foreach {
      i => cluster.serviceScheduler.scheduleExperiment(task :: Nil)
    }
  }

  def setupPartitions(client: MVScaleTest, cluster: ScadsCluster) = {
    val numServers: Int = replicas * partitions
    val available = cluster.getAvailableServers
    assert (available.size >= numServers)

    val groups = available.take(numServers).grouped(replicas).toSeq
    var p: List[(Option[Array[Byte]], Seq[StorageService])] = List()
    for (servers <- groups) {
      if (p.length == 0) {
        p ::= (None, servers)
      } else {
        p ::= (client.serializedPointInKeyspace(p.length, partitions), servers)
      }
    }
    p = p.reverse
    logger.info("Partition scheme: " + p)

    val tags = cluster.getNamespace[Tag]("tags")
    val nn = List(tags,
      tags.getOrCreateIndex(AttributeIndex("item") :: Nil),
      cluster.getNamespace[MTagPair]("mTagPairs"))

    // assume they all have prefixes sampled from the same keyspace
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

    // agh...
    coordination.registerAndAwait("clientsStarted", nClients)

    clients.foreach(client => {
      val clientId = client.getClass.getSimpleName
      val totalItems = itemsPerMachine * partitions
      val scenario = new MVScaleTest(cluster, client, totalItems, totalItems * meanTagsPerItem, maxTagsPerItem, 400)

      if (clientNumber == 0) {
        logger.info("Client %d preparing partitions...", clientNumber)
        setupPartitions(scenario, cluster)
      }
      coordination.registerAndAwait("partitionsReady", nClients)

      /* distributed data load */
      val loadStartMs = System.currentTimeMillis
      scenario.populateSegment(clientNumber, nClients)
      coordination.registerAndAwait("dataReady", nClients)
      val loadTimeMs = System.currentTimeMillis - loadStartMs
      logger.info("Data load wait: %d ms", loadTimeMs)

      (1 to iterations).foreach(iteration => {
        logger.info("Beginning iteration %d", iteration)
        threadCounts.foreach(threadCount => {
          coordination.registerAndAwait("it:" + iteration + ",th:" + threadCount, nClients)
          val iterationStartMs = System.currentTimeMillis
          val failures = new java.util.concurrent.atomic.AtomicInteger()
          var putDelRatio = 0.5
          val histograms = (0 until threadCount).pmap(tid => {
            implicit val rnd = new Random()
            val geth = Histogram(100,10000)
            val puth = Histogram(100,10000)
            val delh = Histogram(100,10000)
            var i = 10000
            while (i > 0) {
              i -= 1
              if (tid == 0 && i % 3000 == 0) {
                val countStart = System.currentTimeMillis
                val count = scenario.client.count
                logger.info("current tag count = " + count
                  + ", count ms = " + (System.currentTimeMillis - countStart))
              }
              try {
                if (rnd.nextDouble() < readFrac) {
                  val respTime = scenario.randomGet
                  logger.debug("Get response time: %d", respTime)
                  geth.add(respTime)
                } else if (rnd.nextDouble() < putDelRatio) {
                  val respTime = scenario.randomPut(maxTagsPerItem)
                  logger.debug("Put response time: %d", respTime)
                  puth.add(respTime)
                } else {
                  val respTime = scenario.randomDel
                  logger.debug("Del response time: %d", respTime)
                  delh.add(respTime)
                }
              } catch {
                case e => 
                  logger.warning(e.getMessage)
                  failures.getAndAdd(1)
              }
            }
            (geth, puth, delh)
          })

          val r = ParResult(System.currentTimeMillis, hostname, iteration, clientId)
          r.threadCount = threadCount
          r.clientNumber = clientNumber
          r.nClients = nClients
          r.replicas = replicas
          r.partitions = partitions
          r.itemsPerMachine = itemsPerMachine
          r.maxTags = maxTagsPerItem
          r.meanTags = meanTagsPerItem
          r.loadTimeMs = loadTimeMs
          r.runTimeMs = System.currentTimeMillis - iterationStartMs
          r.readFrac = readFrac
          var h = histograms.reduceLeft((a, b) => (a._1 + b._1, a._2 + b._2, a._3 + b._3))
          r.getTimes = h._1
          r.putTimes = h._2
          r.delTimes = h._3
          r.failures = failures.get()
          results.put(r)
        })
      })
    })

    coordination.registerAndAwait("experimentDone", nClients)
    cluster.shutdown
  }
}
