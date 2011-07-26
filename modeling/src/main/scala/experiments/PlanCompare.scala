package edu.berkeley.cs
package scads
package piql
package modeling

import deploylib.mesos._
import perf._
import comm._
import storage._
import avro.runtime._
import avro.marker._
import org.apache.zookeeper.CreateMode

import edu.berkeley.cs.scads.piql.scadr.Subscription

case class PlanCompareResult(var hostname: String,
			     var timestamp: Long,
			     var iteration: Int,
			     var point: Int,
			     var query: String,
			     var config: PlanCompareTask) extends AvroPair {
  var responseTimes: Histogram = null
}
		  
object PlanCompare {
  import Experiments._

  def newScadsCluster(size: Int): ScadsCluster = {
    val clusterRoot = cluster.zooKeeperRoot.getOrCreate("scads").createChild("experimentCluster", mode = CreateMode.PERSISTENT_SEQUENTIAL)
    val serverProcs = Array.fill(size)(ScalaEngineTask(clusterAddress=clusterRoot.canonicalAddress).toJvmTask)

    cluster.serviceScheduler.scheduleExperiment(serverProcs)
    new ScadsCluster(clusterRoot)
  }

  def run: Unit = {
    val scadsCluster = newScadsCluster(2)
    val compareTask = PlanCompareTask(
      clusterAddress = scadsCluster.root.canonicalAddress,
      resultClusterAddress = resultClusterAddress.canonicalAddress
    ).toJvmTask
    serviceScheduler.scheduleExperiment(compareTask :: Nil)
  }

  def testLocal =
    PlanCompareTask(
      clusterAddress = TestScalaEngine.newScadsCluster(2).root.canonicalAddress,
      resultClusterAddress = resultClusterAddress.canonicalAddress
    ).run()

  val results = resultsCluster.getNamespace[PlanCompareResult]("planCompareResults")
  def allResults = results.iterateOverRange(None,None)
  def goodResults = allResults

  def graphPoints(quantile: Double = 0.99) = goodResults.toSeq
    .groupBy(r => (r.query, r.point * r.config.scaleStep)).toSeq
    .map { case ((query, size), data) => (query, size, data.map(_.responseTimes).reduceLeft(_ + _).quantile(quantile)) }
    .sortBy(r => (r._1, r._2))

  def backupData = allResults.toAvroFile("planCompare" + System.currentTimeMillis + ".avro")

  def queryCounts = allResults.toSeq
    .groupBy(r => (r.point, r.query)).toSeq
    .map(r => (r._1._1, r._1._2 , r._2.map(_.responseTimes.totalRequests).sum))
    .sortBy(r => r._1)
    
}
   
case class PlanCompareTask(var clusterAddress: String,
			   var resultClusterAddress: String,
			   var replicationFactor: Int = 2,
			   var iterations: Int = 10,
			   var points: Int = 15,
			   var scaleStep: Int = 10,
			   var numExecutions: Int = 1000) extends AvroTask with AvroRecord {


  def run(): Unit = {
    val cluster = new ExperimentalScadsCluster(ZooKeeperNode(clusterAddress))
    cluster.blockUntilReady(replicationFactor)

    val resultCluster = new ScadsCluster(ZooKeeperNode(resultClusterAddress))
    val results = resultCluster.getNamespace[PlanCompareResult]("planCompareResults")
    
    /**
     * Create the partition scheme for subscriptions to be a single partition over
     * the specified number of replicas
     */
    val partitions = (None, cluster.getAvailableServers.take(replicationFactor)) :: Nil
    val subscriptions = cluster.getNamespace[Subscription]("subscriptions")
    val idxTargetSubscriptions = subscriptions.getOrCreateIndex(AttributeIndex("target") :: Nil)
    
    val limit = FixedLimit(50)
    val executor = new ParallelExecutor()

    val naiveQuery =
      new OptimizedQuery(
	"NaiveFollowing",
	   LocalStopAfter(limit,
	     LocalSelection(EqualityPredicate(AttributeValue(0,1), ParameterValue(0)),
	       IndexScan(subscriptions, Nil, FixedLimit(1000), true))),
        executor)
   
    val piqlQuery =
      new OptimizedQuery(
	"PiqlFollowing",
	LocalStopAfter(limit,
	  IndexLookupJoin(subscriptions, AttributeValue(0,1) :: AttributeValue(0,0) :: Nil, 
	    IndexScan(
	      subscriptions.getOrCreateIndex(AttributeIndex("target") :: Nil),
	      ParameterValue(0) :: Nil,
	      limit,
	      true))),
	executor)
    val queries = naiveQuery :: piqlQuery :: Nil

    val rand = new scala.util.Random
    (1 to iterations).foreach(iteration => {
      logger.info("Begining iteration %d", iteration)
      /* clear namespaces by reseting partition scheme */
      subscriptions.delete()
      subscriptions.open()
      idxTargetSubscriptions.delete()
      subscriptions.getOrCreateIndex(AttributeIndex("target") :: Nil)
      idxTargetSubscriptions.open()

      subscriptions.setPartitionScheme(partitions)
      idxTargetSubscriptions.setPartitionScheme(partitions)

      assert(subscriptions.getRange(None, None, limit=1).size == 0)
      assert(idxTargetSubscriptions.getRange(None, None, limit=1).size == 0)

      (1 to points).foreach(point => {
	/* bulkload more subscriptions */
	val minUser = (point - 1) * scaleStep + 1
	val maxUser = point * scaleStep
	logger.info("Measuring with %d users", maxUser)

	logger.info("Loading data for users %d to %d", minUser, maxUser)
	val newUsers = (minUser to maxUser).view
	val followers = (1 to 50).view
	val data = newUsers.flatMap(target => 
	            followers.map(owner => Subscription(toUser(owner), toUser(target))))
	subscriptions ++= data

	logger.info("Data size: %d, Index Size: %d", subscriptions.iterateOverRange(None,None).size, idxTargetSubscriptions.iterateOverRange(None,None).size)

	if(iteration == 1 && point == 1) {
	  logger.info("Begining warmup")
	  val startTime = currentTime
	  while(currentTime - startTime < 3 * 60 * 1000) {
	    queries.foreach(q => q(toUser(1)))
	  }
	}

	/* measure response time for each query given the current state of the db */
	results ++=  queries.map(query => {
	  logger.info("Running %s %d times", query.name, numExecutions)
	  val responseTimes = Histogram(1,1000)

	  val startMessages = MessageHandler.futureCount
	  (1 to numExecutions).foreach(i => {
	    val username = toUser(rand.nextInt(maxUser) + 1)

	    val startTime = currentTime
	    val answer = query(username)
	    logger.debug("Query Result: %s", answer)
	    assert(answer.size == 50)
	    var endTime = currentTime

	    responseTimes.add(endTime - startTime)
	  })
	  val endMessages = MessageHandler.futureCount
	  logger.info("Messages sent: %d", endMessages - startMessages)

	  val result = PlanCompareResult(
	    java.net.InetAddress.getLocalHost.getHostName,
	    System.currentTimeMillis,
	    iteration,
	    point,
	    query.name.getOrElse("unnamed"),
	    this)
	  result.responseTimes = responseTimes
	  result
	})	
      })
    })	
  }
  def currentTime = System.nanoTime / 1000000
  def toUser(i: Int) = "User%010d".format(i)
}
