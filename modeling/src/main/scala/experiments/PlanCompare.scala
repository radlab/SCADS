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

  def graphPoints = goodResults.map(r => (r.point * r.config.scaleStep, r.responseTimes.quantile(0.99))).toSeq

}
   
case class PlanCompareTask(var clusterAddress: String,
			   var resultClusterAddress: String,
			   var replicationFactor: Int = 2,
			   var iterations: Int = 5,
			   var points: Int = 10,
			   var scaleStep: Int = 1,
			   var runLengthMin: Int = 1) extends AvroTask with AvroRecord {


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
	       IndexScan(subscriptions, Nil, limit, true))),
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
      subscriptions.setPartitionScheme(partitions)
      idxTargetSubscriptions.setPartitionScheme(partitions)

      (1 to points).foreach(point => {
	/* bulkload more subscriptions */
	val minUser = (point - 1) * scaleStep
	val maxUser = point * scaleStep
	logger.info("Measuring with %d users", maxUser)
	val newUsers = (minUser to maxUser).view
	val followers = (1 to 50).view
	val data = newUsers.flatMap(target => 
	            followers.map(owner => Subscription(toUser(owner), toUser(target))))
	subscriptions ++= data

	/* measure response time for each query given the current state of the db */
	results ++=  queries.map(query => {
	  logger.info("Running %s for %d minutes", query.name, runLengthMin)
	  val startTime = currentTime
	  val responseTimes = Histogram(1,1000)
	  var iterStartTime = currentTime
	  while(iterStartTime - startTime < runLengthMin * 60 * 1000) {
	    val username = toUser(rand.nextInt(maxUser))
	    query(username)
	    var endTime = currentTime
	    responseTimes.add(endTime - iterStartTime)
	    iterStartTime = endTime
	  }
	  val result = PlanCompareResult(java.net.InetAddress.getLocalHost.getHostName,
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
