package edu.berkeley.cs
package scads
package piql
package modeling

import comm._
import storage._
import perf._
import deploylib.ec2._
import deploylib.mesos._
import piql.scadr._
import perf.scadr._
import avro.marker._
import avro.runtime._

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
  def debug(pkg: String) = net.lag.logging.Logger(pkg).setLevel(java.util.logging.Level.FINEST)
  def expCluster(expId: Int) = 
    new ScadsCluster(
      ZooKeeperNode("zk://zoo.knowsql.org/home/marmbrus/scads/experimentCluster%010d".format(expId)))

  def scadrClient(expId: Int) = 
    new ScadrClient(expCluster(expId), new ParallelExecutor)

  lazy val scadsCluster = new ScadsCluster(traceRoot)
  lazy val scadrClient = new piql.scadr.ScadrClient(scadsCluster, new ParallelExecutor)
  lazy val testScadrClient = {
    val cluster = TestScalaEngine.newScadsCluster()
    val client = new piql.scadr.ScadrClient(cluster, new ParallelExecutor with DebugExecutor)
    val loader = new piql.scadr.ScadrLoader(1, 1)
    loader.getData(0).load(client)
    client
  }

  def clients = cluster.slaves.pflatMap(_.jps).filter(_.main equals "AvroTaskMain").pfilterNot(_.stack contains "ScalaEngineTask")
  def laggards = clients.pfilterNot(_.stack contains "awaitChild")
  def tagClients = clients.map(_.remoteMachine.asInstanceOf[EC2Instance]).foreach(_.tags += ("task", "client"))

  def killTask(id: Int): Unit = cluster.serviceScheduler !? KillTaskRequest(id)

  object QueryRunner {
    val results = resultsCluster.getNamespace[Result]("queryRunnerResults")
    def downloadResults: Unit = {
      val outfile = AvroOutFile[Result]("QueryRunnerResults.avro")
      results.iterateOverRange(None, None).foreach(outfile.append)
      outfile.close
    }

    def allResults = AvroInFile[Result]("QueryRunnerResults.avro")
    def goodResults = allResults.filter(_.failedQueries < 200)
				.filterNot(_.iteration == 1)
				.filter(_.clientConfig.iterationLengthMin == 10)
				.filter(_.clientConfig.numClients == 50)

    def queryTypeQuantile(results: Seq[Result] = goodResults.toSeq, quantile: Double = 0.90) =
      results.groupBy(_.queryDesc).map {
	case (queryDesc, results) => (queryDesc, results.map(_.responseTimes)
							.reduceLeft(_ + _)
							.map(_.quantile(quantile)))
      }

    def thoughtstreamGraph =
      queryTypeQuantile().filter(_._1.queryName == "thoughtstream")
    
		def quantileCsv(queryName: String) = {
		  val quantiles = queryTypeQuantile().filter(_._1.queryName == queryName)
		  
		  quantiles.map(i => {
		    var line:List[String] = Nil
		    line = i._1.queryName :: line
		    
		    (1 to i._1.parameters.length).foreach(j =>
		      line = i._1.parameters(j-1).toString :: line
		    )
		    
		    line = i._2.get.toString :: line
		    println(line.reverse.mkString(","))
		  })
		}
		
		def thoughtstreamQuantileCsv = {
		  println("queryName,numSubscriptions,numPerPage,latency")
		  quantileCsv("thoughtstream")
		}
		
		def myThoughtsQuantileCsv = {
		  println("queryName,numPerPage,latency")
		  quantileCsv("myThoughts")
		}

		def usersFollowedByQuantileCsv = {
		  println("queryName,numSubscriptions,latency")
		  quantileCsv("usersFollowedBy")
		}
		
    def defaultScadr = ScadrLoaderTask(numServers=50,
				       numLoaders=50,
				       followingCardinality=500,
				       replicationFactor=1,
				       usersPerServer=20000,
				       thoughtsPerUser=100
				     )

    def testScadrCluster = ScadrLoaderTask(numServers=10,
				  numLoaders=10,
				  followingCardinality=100,
				  replicationFactor=1,
				  usersPerServer=100,
				  thoughtsPerUser=100
				 )
    val defaultRunner = 
      QueryRunnerTask(50,
		      "edu.berkeley.cs.scads.piql.modeling.ScadrQueryProvider",
		      iterations=30,
		      iterationLengthMin=10,
		      threads=2,
		      traceIterators=false,
		      traceMessages=false,
		      traceQueries=false)

    def testReplicationEffect: Unit = {
      val rep = defaultScadr.copy(replicationFactor=2).newCluster
      val noRep = defaultScadr.newCluster
      benchmarkScadr(rep)
      benchmarkScadr(noRep)
    }

    def testGenericRunner: Unit = {
      val cluster = GenericRelationLoaderTask(4,2,2,10,1,500).newTestCluster
      val runner = QueryRunnerTask(1,
		      "edu.berkeley.cs.scads.piql.modeling.GenericQueryProvider",
		      iterations=10,
		      iterationLengthMin=1,
		      threads=2,
		      traceIterators=false,
		      traceMessages=false,
		      traceQueries=false)
      runner.clusterAddress = cluster.root.canonicalAddress
      runner.experimentAddress = cluster.root.canonicalAddress
      runner.resultClusterAddress = cluster.root.canonicalAddress
      while(cluster.getAvailableServers.size < 1) Thread.sleep(100)
      val thread = new Thread(runner)
      thread.start
    }

    def genericCluster = 
      GenericRelationLoaderTask(
	numServers=10,
	numLoaders=10,
	replicationFactor=2,
	tuplesPerServer=10000,
	dataSize=10,
	maxCardinality=500)
    
    def defaultGenericRunner =
       QueryRunnerTask(numClients=10,
		      "edu.berkeley.cs.scads.piql.modeling.GenericQueryProvider",
		      iterations=30,
		      iterationLengthMin=10,
		      threads=2,
		      traceIterators=false,
		      traceMessages=false,
		      traceQueries=false)

    def scadrGenericBenchmark = {
      val clust30 = genericCluster.copy(dataSize=30).newCluster
      val clust40 = genericCluster.copy(dataSize=40).newCluster

      genericBenchmark(clust30)
      genericBenchmark(clust40)
    }

    def genericBenchmark(cluster: ScadsCluster = genericCluster.newCluster) =
      defaultGenericRunner.schedule(cluster, resultsCluster)

    def benchmarkScadr(cluster: ScadsCluster = defaultScadr.newCluster) =
      defaultRunner.schedule(cluster,resultsCluster)
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
      new ScatterPlot(results.iterateOverRange(None, None)
			     .filter(_.iteration != 1).toSeq
			     .groupBy(_.clientConfig.threads)
			     .map {
			       case (threads, results) => 
				 (threads, results.map(_.times)
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
