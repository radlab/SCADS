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
import storage.transactions._

import net.lag.logging.Logger

/* task to run MVScaleTest on EC2 */
case class ScaleTask(var replicas: Int = 1,
                     var partitions: Int = 8,
                     var nClients: Int = 8,
                     var iterations: Int = 2,
                     var itemsPerMachine: Int = 500000,
                     var maxTagsPerItem: Int = 10,
                     var meanTagsPerItem: Int = 4,
                     var readFrac: Double = 0.8,
                     var threadCount: Int = 32,
                     var comment: String = "")
            extends AvroTask with AvroRecord with TaskBase {
  
  var resultClusterAddress: String = _
  var clusterAddress: String = _

  def schedule(resultClusterAddress: String)(implicit cluster: deploylib.mesos.Cluster,
                                             classSource: Seq[ClassSource]): Unit = {
    var extra = java.lang.Math.sqrt(replicas * partitions / 5).intValue
    /* preallocSize = 4GiB */
    val scadsCluster = newScadsCluster(replicas * partitions + extra, 1L << 32)
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
    val results = resultCluster.getNamespace[ParResult3]("ParResult3")

    val hostname = java.net.InetAddress.getLocalHost.getHostName

    // XXX support concurrent runs with different partition counts
    val coordination = cluster.root.getOrCreate("coordination/mvscale:id:" + partitions)
    val clientNumber = coordination.registerAndAwait("clientsStart", nClients)

    // setup client (AFTER namespace creation)
    val client = new MTagClient(cluster, new ParallelExecutor)

    coordination.registerAndAwait("clientsStarted", nClients)
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
      coordination.registerAndAwait("it:" + iteration + ",th:" + threadCount, nClients)
      val iterationStartMs = System.currentTimeMillis
      val failures = new java.util.concurrent.atomic.AtomicInteger()
      val histograms = (0 until threadCount).pmap(tid => {
        implicit val rnd = new Random()
        val tfrac: Double = tid.doubleValue / threadCount.doubleValue
        val geth = Histogram(1000,1000)
        val puth = Histogram(1000,5000)
        val nvputh = Histogram(1000,1000)

        /* don't care for now */
        val delh = Histogram(1000,1)
        val nvdelh = Histogram(1000,1)

        while (System.currentTimeMillis - iterationStartMs < 3*60*1000) {
          try {
            if (rnd.nextDouble() < readFrac) {
              val respTime = scenario.randomGet
              geth.add(respTime)
            } else {
              val (noViewRespTime, respTime) = scenario.randomPut(maxTagsPerItem)
              logger.info("put time " + respTime)
              puth.add(respTime)
              nvputh.add(noViewRespTime)
            }
          } catch {
            case e => 
              logger.error(e.getMessage)
              failures.getAndAdd(1)
          }
        }
        (geth, puth, delh, nvputh, nvdelh)
      })

      val r = ParResult3(System.currentTimeMillis, hostname, iteration, clientId)
      r.threadCount = threadCount
      r.clientNumber = clientNumber
      r.nClients = nClients
      r.replicas = replicas
      r.partitions = partitions
      r.itemsPerMachine = itemsPerMachine
      r.maxTags = maxTagsPerItem
      r.meanTags = meanTagsPerItem
      r.loadTimeMs = loadTimeMs
      r.comment = comment
      r.runTimeMs = System.currentTimeMillis - iterationStartMs
      r.readFrac = readFrac
      var h = histograms.reduceLeft((a, b) => (a._1 + b._1, a._2 + b._2, a._3 + b._3, a._4 + b._4, a._5 + b._5))
      r.getTimes = h._1
      r.putTimes = h._2
      r.delTimes = h._3
      r.nvputTimes = h._4
      r.nvdelTimes = h._5
      r.failures = failures.get()
      results.put(r)

      val countStart = System.currentTimeMillis
      val count = scenario.client.count
      logger.info("current tag count = " + count
        + ", count ms = " + (System.currentTimeMillis - countStart))
    })

    coordination.registerAndAwait("experimentDone", nClients)
    if (clientNumber == 0) {
      cluster.shutdown
    }
  }
}
