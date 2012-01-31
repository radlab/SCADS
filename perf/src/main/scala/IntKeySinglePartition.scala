package edu.berkeley.cs
package scads
package perf

import deploylib._
import deploylib.mesos._
import deploylib.ec2._
import avro.marker._
import comm._
import config._
import storage._

import net.lag.logging.Logger

case class Result2(var hostname: String,
                  var timestamp: Long,
                  var iteration: Int,
                  var dataSize: Int,
                  var recordCount: Int,
                  var threadCount: Int) extends AvroPair {
  var lastCount: Int = _
  var loadTimeMs: Long = _
  var runTimeMs: Long =  _
  var responseTimes: Histogram = null
  var failures: Int = _
  var replicationFactor: Int = _
  var nClients: Int = _

  def fmt: String = {
      val x = ("lat(0.5)=" + responseTimes.quantile(0.5),
               "lat(0.99)=" + responseTimes.quantile(0.99),
               "gets/s=" + responseTimes.totalRequests*1.0/runTimeMs*1000,
               "threads=" + threadCount)
      val s = x.toString
      return s.replaceAll(",","\t").substring(1, s.length-1)
  }
}

object Experiment extends ExperimentBase {
  def storageTippingPoint(implicit cluster: deploylib.mesos.Cluster, classSource: Seq[ClassSource]): Unit = {
    val done = graphPoints.groupBy(_._1).filter(_._2.size >= 2).map(_._1).toSet
    Task(recordCounts = (1 to 9).map(math.pow(10, _).toInt).filterNot(done.contains)).schedule(resultClusterAddress)
  }

  def zoomIn(implicit cluster: deploylib.mesos.Cluster, classSource: Seq[ClassSource]): Unit = {
    Task(recordCounts = (40000000 to 100000000 by 5000000)).schedule(resultClusterAddress)
  }

  def biggerRecs(implicit cluster: deploylib.mesos.Cluster, classSource: Seq[ClassSource]): Unit = {
    Task(recordCounts = (1 to 9).map(math.pow(10, _).toInt), dataSizes = Seq(64)).schedule(resultClusterAddress)
  }

  def singleNodeRead(implicit cluster: deploylib.mesos.Cluster, classSource: Seq[ClassSource]): Unit = {
    Task(1,1,100000,Seq(0,1024),Seq(1),Seq(1,2,4,8,16,32)).schedule(resultClusterAddress)
  }

  def dualNodeRead(implicit cluster: deploylib.mesos.Cluster, classSource: Seq[ClassSource]): Unit = {
    Task(1,1,100000,Seq(0,1024,4096),Seq(3),Seq(32),2).schedule(resultClusterAddress)
  }

  def quadNodeRead(implicit cluster: deploylib.mesos.Cluster, classSource: Seq[ClassSource]): Unit = {
    Task(1,1,100000,Seq(0,1024,4096),Seq(4),Seq(32),4).schedule(resultClusterAddress)
  }

  def octNodeRead(implicit cluster: deploylib.mesos.Cluster, classSource: Seq[ClassSource]): Unit = {
    Task(1,1,100000,Seq(0),Seq(5),Seq(16),8).schedule(resultClusterAddress)
  }

  def bigDB(implicit cluster: deploylib.mesos.Cluster, classSource: Seq[ClassSource]): Unit = {
    Task(1,2,25000,Seq(8192),Seq(625000),Seq(1),1).schedule(resultClusterAddress)
  }


  def manyToMany(implicit cluster: deploylib.mesos.Cluster, classSource: Seq[ClassSource]): Unit = {
    val nClients = 1;
    val replicationFactor = 1;
    val task = Task(replicationFactor,2,100000,Seq(64),Seq(10*1024),Seq(1),nClients)
    task.schedule(resultClusterAddress)
  }

  def reset(implicit cluster: deploylib.mesos.Cluster, classSource: Seq[ClassSource]) = {
    results.delete()
    results.open()
    cluster.restart
  } 

  lazy val results = resultCluster.getNamespace[Result2]("singleNodeResult")
  def goodResults = results.iterateOverRange(None,None)

  def graphPoints = {
    goodResults.toSeq
      .groupBy(r => (r.recordCount, r.threadCount))
      .map {
        case ((recs, threads), results) =>
          val aggHist = results.map(_.responseTimes).reduceLeft(_ + _)
          (recs, threads, aggHist.quantile(0.50), aggHist.quantile(0.99), aggHist.totalRequests, aggHist.negative, results.size)
      }
  }

    def main(args: Array[String]): Unit = {
      val task = new Task()
      val logger = Logger()
      implicit val cluster = new Cluster(USWest2)
      logger.info("MAIN")
      implicit val classSource = cluster.classSource
      logger.info("DONE")
      logger.info("classSource " + classSource)
      logger.info("cluster " + cluster)
      logger.info("resultClusterAddress " + resultClusterAddress)
      task.schedule(resultClusterAddress)
      graphPoints.toSeq.sortBy(r => (r._2, r._1)).foreach(println)
    }
}

case class Record(var f1: Int) extends AvroPair {
  var f2: String  = ""
}

case class Task(var replicationFactor: Int = 2,
                var iterations: Int = 20,
                var getCount: Int = 100000,
                var dataSizes: Seq[Int] = Seq(0),
                var recordCounts: Seq[Int] = Seq(1024*10),
                var threadCounts: Seq[Int] = Seq(1, 5),
                var nClients: Int = 1)
     extends AvroTask with AvroRecord with TaskBase {
  
  var resultClusterAddress: String = _
  var clusterAddress: String = _

  def schedule(resultClusterAddress: String)(implicit cluster: deploylib.mesos.Cluster, classSource: Seq[ClassSource]): Unit = {
    val scadsCluster = newScadsCluster(replicationFactor)
    clusterAddress = scadsCluster.root.canonicalAddress
    this.resultClusterAddress = resultClusterAddress
    val task = this.toJvmTask
    (1 to nClients).foreach {
      i => cluster.serviceScheduler.scheduleExperiment(task :: Nil)
    }
  }

  def run(): Unit = {
    val logger = Logger()
    val cluster = new ExperimentalScadsCluster(ZooKeeperNode(clusterAddress))
    cluster.blockUntilReady(replicationFactor)

    val coordination = cluster.root.getOrCreate("coordination/loaders")
    val resultCluster = new ScadsCluster(ZooKeeperNode(resultClusterAddress))
    val results = resultCluster.getNamespace[Result2]("singleNodeResult")

    val clientId = coordination.registerAndAwait("clientsStart", nClients)

   /**
     * Create the partition scheme for subscriptions to be a single partition over
     * the specified number of replicas
     */
    if (clientId == 0) {
      logger.info("Creating partitions")
      val partitions = (None, cluster.getAvailableServers.take(replicationFactor)) :: Nil
      val nn = cluster.createNamespace[Record](this.getClass.getName, partitions)
      nn.setPartitionScheme(partitions)
      nn.setReadWriteQuorum(.001, 1.00)
    }

    coordination.registerAndAwait("storageReady", nClients)
    val ns = cluster.getNamespace[Record](this.getClass.getName)

    val hostname = java.net.InetAddress.getLocalHost.getHostName
    var loaded = false
    (1 to iterations).foreach(iteration => {
      dataSizes.foreach {case dataSize => 
        (0 +: recordCounts).sliding(2).foreach { case lastCount :: currentCount :: Nil =>
          logger.info("loading data from %d to %d", lastCount, currentCount)
          require(lastCount < currentCount, "record count must me monotonicaly increasing")
          val data = "*" * dataSize
          val loadStart = System.currentTimeMillis
          if (!loaded)
            ns ++= (lastCount to currentCount).view.map(i => {val r = Record(i); r.f2 = data; r})  
          else
            logger.info("-- skipped data load --")
          val loadEnd = System.currentTimeMillis
          threadCounts.foreach( threadCount => {
            logger.info("Begining test: iteration %d, %d records, %d threads", iteration, currentCount, threadCount)
            def currentTime = System.nanoTime / 1000

            val failures = new java.util.concurrent.atomic.AtomicInteger()
            val startTime = System.currentTimeMillis
            val histograms = (0 until threadCount).pmap(i => {
              val rand = new scala.util.Random
              val histogram = Histogram(10,5000)
              var i = getCount
              var lastTime = currentTime
              while(i > 0) {
                i -= 1
                try {
                  ns.get(Record(rand.nextInt(currentCount)))
                  val endTime = currentTime
                  val respTime = endTime - lastTime
                  logger.debug("Put response time: %d", respTime)
                  histogram.add(respTime)
                  lastTime = endTime 
                }
                catch {
                  case e => 
                    failures.getAndAdd(1)
                    lastTime = currentTime
                }
              }
              histogram
            })
            val result = Result2(hostname, System.currentTimeMillis, iteration, dataSize, currentCount, threadCount)
            result.lastCount = lastCount
            result.loadTimeMs = loadEnd - loadStart
            result.runTimeMs = System.currentTimeMillis - startTime
            result.responseTimes = histograms.reduceLeft(_ + _)
            result.failures = failures.get()
            result.replicationFactor = replicationFactor
            result.nClients = nClients
            results.put(result)
          })
        }
        loaded = true
      }
    })                                                                      
  }
}
