package edu.berkeley.cs
package scads
package perf
package intkey.singlenode

import deploylib._
import deploylib.mesos._
import avro.marker._
import comm._
import config._
import storage._

import net.lag.logging.Logger

case class Result(var hostname: String,
		  var timestamp: Long,
		  var iteration: Int,
		  var recordCount: Int,
		  var threadCount: Int) extends AvroPair {

  var runTimeMs: Long =  _
  var responseTimes: Histogram = null
  var failures: Int = _
}

object Experiment extends ExperimentBase {
  def run(implicit cluster: deploylib.mesos.Cluster, classSource: Seq[ClassSource]): Unit = {
    val scadsCluster = newScadsCluster(2)
    val task = Task(scadsCluster.root.canonicalAddress,
	 resultClusterAddress).toJvmTask
    cluster.serviceScheduler.scheduleExperiment(task :: Nil)
  }

  lazy val results = resultCluster.getNamespace[Result]("singleNodeResult")
  def goodResults = results.iterateOverRange(None,None)

  def graphPoints = {
    goodResults.toSeq
      .groupBy(r => (r.recordCount, r.threadCount))
      .map {
	case ((recs, threads), results) =>
	  val aggHist = results.map(_.responseTimes).reduceLeft(_ + _)
	  (recs, threads, aggHist.quantile(0.50), aggHist.totalRequests, aggHist.negative, results.size)
      }
  }

    def main(args: Array[String]): Unit = {
 //     val cluster = TestScalaEngine.newScadsCluster(2)
 //     val task = new Task(cluster.root.canonicalAddress,
//			  cluster.root.canonicalAddress).run()

      graphPoints.toSeq.sortBy(r => (r._2, r._1)).foreach(println)
    }
		   
}

case class Task(var clusterAddress: String,
		var resultClusterAddress: String,
		var replicationFactor: Int = 2,
		var iterations: Int = 20,
		var getCount: Int = 100000,
		var recordCounts: Seq[Int] = (10000000 to 100000000 by 5000000).toSeq, //(1 to 9).map(math.pow(10, _)).map(_.toInt),
		var threadCounts: Seq[Int] = Seq(1, 5))
     extends AvroTask with AvroRecord {

  def run(): Unit = {
    val logger = Logger()
    val cluster = new ExperimentalScadsCluster(ZooKeeperNode(clusterAddress))
    cluster.blockUntilReady(replicationFactor)

    val resultCluster = new ScadsCluster(ZooKeeperNode(resultClusterAddress))
    val results = resultCluster.getNamespace[Result]("singleNodeResult")

   /**
     * Create the partition scheme for subscriptions to be a single partition over
     * the specified number of replicas
     */
    logger.info("Creating partitions")
    val partitions = (None, cluster.getAvailableServers.take(replicationFactor)) :: Nil
    val ns = cluster.createNamespace[IntRec, IntRec](this.getClass.getName, partitions)

    val hostname = java.net.InetAddress.getLocalHost.getHostName
    (1 to iterations).foreach(iteration => {
      ns.setPartitionScheme(partitions)
      require(ns.getRange(None, None, limit=Some(10)).size == 0, "Namespace not empty")
      (0 +: recordCounts).sliding(2).foreach { case lastCount :: currentCount :: Nil =>
	logger.info("loading data from %d to %d", lastCount, currentCount)
	require(lastCount < currentCount, "record count must me monotonicaly increasing")
	ns ++= (lastCount to currentCount).view.map(i => (IntRec(i), IntRec(i)))  
	threadCounts.foreach( threadCount => {
	  logger.info("Begining test: iteration %d, %d records, %d threads", iteration, currentCount, threadCount)
	  def currentTime = System.nanoTime / 1000

	  val failures = new java.util.concurrent.atomic.AtomicInteger()
	  val startTime = System.currentTimeMillis
	  val histograms = (0 until threadCount).pmap(i => {
	    val rand = new scala.util.Random
	    val histogram = Histogram(100,1000)
	    var i = getCount
	    var lastTime = currentTime
	    while(i > 0) {
	      i -= 1
	      try {
		ns.get(IntRec(rand.nextInt(currentCount)))
		val endTime = currentTime
		val respTime = endTime - lastTime
		logger.debug("Get response time: %d", respTime)
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
	  val result = Result(hostname, System.currentTimeMillis, iteration, currentCount, threadCount)
	  result.runTimeMs = currentTime - startTime
	  result.responseTimes = histograms.reduceLeft(_ + _)
	  result.failures = failures.get()
	  results.put(result)
	})
      }
    })					  
  }
}
