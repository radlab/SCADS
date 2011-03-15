package edu.berkeley.cs
package scads
package piql
package modeling

import comm._
import storage._
import deploylib.mesos._

object Experiments {
  implicit var zooKeeperRoot = ZooKeeperNode("zk://zoo.knowsql.org/").getOrCreate("home").getOrCreate(System.getenv("USER"))
  val cluster = new Cluster(zooKeeperRoot)
  val resultsCluster = new ScadsCluster(zooKeeperRoot.getOrCreate("results"))

  object results extends deploylib.ec2.AWSConnection {
    import collection.JavaConversions._
    import com.amazonaws.services.s3._
    import model._

    val client = new AmazonS3Client(credentials)

    def listFiles(bucket: String, prefix: String) = {
      client.listObjects((new ListObjectsRequest).withBucketName(bucket)
						 .withPrefix(prefix))
	    .getObjectSummaries.map(_.getKey)
    }
  }

  implicit def classSource = cluster.classSource
  implicit def serviceScheduler = cluster.serviceScheduler
  def traceRoot = zooKeeperRoot.getOrCreate("traceCollection")

  lazy val scadsCluster = new ScadsCluster(traceRoot)
  lazy val scadrClient = new scadr.ScadrClient(scadsCluster, new ParallelExecutor)
  lazy val testScadrClient = {
    val cluster = TestScalaEngine.newScadsCluster()
    val client = new scadr.ScadrClient(cluster, new ParallelExecutor)
    val loader = new scadr.ScadrLoader(client, 1, 1)
    loader.getData(0).load
    client
  }

  def laggards = cluster.slaves.pflatMap(_.jps).filter(_.main equals "AvroTaskMain").pfilterNot(_.stack contains "ScalaEngineTask").pfilterNot(_.stack contains "awaitChild")

  def killTask(id: Int): Unit = cluster.serviceScheduler !? KillTaskRequest(id)

  def scadrClusterParams = ScadrClusterParams(
    traceRoot.canonicalAddress, // cluster address
    50,                         // num storage nodes
    50,                         // num load clients
    100,                        // num per page
    1000000,                    // num users
    100,                        // num thoughts per user
    1000                        // num subscriptions per user
  )

  def thoughtstreamRunParams = RunParams(
    scadrClusterParams,
    "thoughtstream",
    "thoughtstream-newclient",
    50                          // # trace collectors
  )

  def localUserThoughtstreamRunParams = RunParams(
    scadrClusterParams,
    "localUserThoughtstream",
    "localUserThoughtstream-newclient",
    50                          // # trace collectors
  )

  def startScadrTraceCollector: Unit = {
    val traceTask = ScadrTraceCollectorTask(
      RunParams(
        scadrClusterParams,
        "mySubscriptions"
      )
    ).toJvmTask
    
    serviceScheduler !? RunExperimentRequest(traceTask :: Nil)
  }

  def startThoughtstreamTraceCollector: Unit = {
    val traceTasks = Array.fill(thoughtstreamRunParams.numTraceCollectors)(ThoughtstreamTraceCollectorTask(thoughtstreamRunParams).toJvmTask)
    
    serviceScheduler !? RunExperimentRequest(traceTasks.toList)
  }

  def startOneThoughtstreamTraceCollector: Unit = {
    val traceTask = ThoughtstreamTraceCollectorTask(thoughtstreamRunParams).toJvmTask
    
    serviceScheduler !? RunExperimentRequest(traceTask :: Nil)
  }

  def startLocalUserThoughtstreamTraceCollector: Unit = {
    val traceTasks = Array.fill(localUserThoughtstreamRunParams.numTraceCollectors)(ThoughtstreamTraceCollectorTask(localUserThoughtstreamRunParams).toJvmTask)
    
    serviceScheduler !? RunExperimentRequest(traceTasks.toList)
  }

  def startOneLocalUserThoughtstreamTraceCollector: Unit = {
    val traceTask = ThoughtstreamTraceCollectorTask(localUserThoughtstreamRunParams).toJvmTask
    
    serviceScheduler !? RunExperimentRequest(traceTask :: Nil)
  }

  def startScadrDataLoad: Unit = {
    val engineTask = ScalaEngineTask(traceRoot.canonicalAddress).toJvmTask
    val loaderTask = ScadrDataLoaderTask(scadrClusterParams).toJvmTask

    val storageEngines = Vector.fill(scadrClusterParams.numStorageNodes)(engineTask)
    val dataLoadTasks = Vector.fill(scadrClusterParams.numLoadClients)(loaderTask)

    serviceScheduler !? (RunExperimentRequest(storageEngines), 30 * 1000)
    serviceScheduler !? (RunExperimentRequest(dataLoadTasks), 30 * 1000)
  }

  object ScadrScaleExperiment {
    import perf.scadr._
    import scale._

    val results = resultsCluster.getNamespace[perf.scadr.scale.Result]("scadrScaleResults")

    def makeScaleGraphPoint(size: Int) = { 
      QueryRunnerTask(size, 
		      "edu.berkeley.cs.scads.piql.ParallelExecutor",
		      0.01,
		      threads=10)
	.schedule(ScadrLoaderTask(size, size, 10).newCluster,
		  resultsCluster)
    }

    def dataSizeResults = results.getRange(None, None)
				 .filter(_.iteration != 1)
				 .groupBy(_.loaderConfig.usersPerServer)
				 .map {
				   case (users, results) => 
				     (users, results.map(_.times).reduceLeft(_+_).quantile(.99))
				 }

    def dataSizes(sizes: Seq[Int] = (2 to 20 by 2).map(_ * 10000)) = {
      sizes.map(numUsers =>
	QueryRunnerTask(5, 
			"edu.berkeley.cs.scads.piql.ParallelExecutor",
			0.01,
			iterations=2,
			runLengthMin=2,
			threads=10)
		.schedule(ScadrLoaderTask(5, 5, usersPerServer=numUsers).newCluster,
		  resultsCluster))
    }
  }
}
