package edu.berkeley.cs
package scads
package piql
package modeling

import comm._
import storage._
import perf._
import deploylib.mesos._
import piql.scadr._
import perf.scadr._

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

  def watchServiceScheduler = cluster.firstMaster.watch("/root/serviceScheduler.log")

  lazy val scadsCluster = new ScadsCluster(traceRoot)
  lazy val scadrClient = new piql.scadr.ScadrClient(scadsCluster, new ParallelExecutor)
  lazy val testScadrClient = {
    val cluster = TestScalaEngine.newScadsCluster()
    val client = new piql.scadr.ScadrClient(cluster, new ParallelExecutor)
    val loader = new piql.scadr.ScadrLoader(client, 1, 1)
    loader.getData(0).load
    client
  }

  def laggards = cluster.slaves.pflatMap(_.jps).filter(_.main equals "AvroTaskMain").pfilterNot(_.stack contains "ScalaEngineTask").pfilterNot(_.stack contains "awaitChild")

  def killTask(id: Int): Unit = cluster.serviceScheduler !? KillTaskRequest(id)

  object QueryRunner {
    val results = resultsCluster.getNamespace[Result]("queryRunnerResults")

    def benchmarkScadr = {
      QueryRunnerTask(50,
		      "edu.berkeley.cs.scads.piql.modeling.ScadrQueryProvider",
		      iterations=30,
		      iterationLengthMin=10,
		      traceIterators=false,
		      traceMessages=false,
		      traceQueries=false)
	.schedule(ScadrLoaderTask(numServers=50,
				  numLoaders=50,
				  followingCardinality=500,
				  replicationFactor=1,
				  usersPerServer=20000,
				  thoughtsPerUser=100
				 ).newCluster,
		  resultsCluster)
    }
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

    def dataSizeResults = 
      new ScatterPlot(results.getRange(None, None)
			     .filter(_.iteration != 1)
			     .filter(_.clientConfig.threads == 10)
			     .groupBy(_.loaderConfig.usersPerServer)
			     .map {
			       case (users, results) => 
				 (users, results.map(_.times)
						.reduceLeft(_+_)
						.quantile(.99))
			     }.toSeq,
		      title="Users/Server vs. Response Time",
		      xaxis="Users per server",
		      yaxis="99th Percentile Response Time",
		      xunit="users/server",
		      yunit="milliseconds")

    def threadCountResults =
      new ScatterPlot(results.getRange(None, None)
			     .filter(_.iteration != 1)
			     .groupBy(_.clientConfig.threads)
			     .map {
			       case (users, results) => 
				 (users, results.map(_.times)
						.reduceLeft(_+_)
						.quantile(.99))
			     }.toSeq,
		     title="ThreadCount vs. Responsetime",
		     xaxis="Number of threads per application server",
		     yaxis="99th Percentile ResponseTime",
		     xunit="threads",
		     yunit="milliseconds")

    def runDataSizes(sizes: Seq[Int] = (2 to 20 by 2).map(_ * 10000)) = {
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

    def runThreads(numThreads: Seq[Int] = (10 to 100 by 10)) = {
      numThreads.map(numThreads =>
	QueryRunnerTask(5, 
			"edu.berkeley.cs.scads.piql.ParallelExecutor",
			0.01,
			iterations=2,
			runLengthMin=2,
			threads=numThreads)
		.schedule(ScadrLoaderTask(5, 5).newCluster,
		  resultsCluster))
    }
  }
}
