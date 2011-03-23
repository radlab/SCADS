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
  def scadrClient(expId: Int) = 
    new ScadrClient(
      new ScadsCluster(
	ZooKeeperNode("zk://zoo.knowsql.org/home/marmbrus/scads/experimentCluster%010d".format(expId
))),
      new ParallelExecutor)

  lazy val scadsCluster = new ScadsCluster(traceRoot)
  lazy val scadrClient = new piql.scadr.ScadrClient(scadsCluster, new ParallelExecutor)
  lazy val testScadrClient = {
    val cluster = TestScalaEngine.newScadsCluster()
    val client = new piql.scadr.ScadrClient(cluster, new ParallelExecutor with DebugExecutor)
    val loader = new piql.scadr.ScadrLoader(1, 1)
    loader.getData(0).load(client)
    client
  }

  def laggards = cluster.slaves.pflatMap(_.jps).filter(_.main equals "AvroTaskMain").pfilterNot(_.stack contains "ScalaEngineTask").pfilterNot(_.stack contains "awaitChild")

  def killTask(id: Int): Unit = cluster.serviceScheduler !? KillTaskRequest(id)

  object QueryRunner {
    import scala.collection.mutable.HashSet
    
    val results = resultsCluster.getNamespace[Result]("queryRunnerResults")
    def downloadResults: Unit = {
      val outfile = AvroOutFile[Result]("QueryRunnerResults.avro")
      results.iterateOverRange(None, None).foreach(outfile.append)
      outfile.close
    }

    // get all of the querytypes in this results set
    def queryTypes(results: Seq[Result] = goodResults.toSeq):HashSet[String] = {
      val set = new HashSet[String]()
      results.foreach(set += _.queryDesc.queryName)
      set
    }
    
    def allResults = AvroInFile[Result]("QueryRunnerResults.avro")
    
    def experimentResults = allResults.filter(_.clientConfig.experimentAddress contains "experiment0000000164")
    
    def goodExperimentResults = experimentResults.filter(_.failedQueries < 200)
				.filterNot(_.iteration == 1)
				.filter(_.clientConfig.iterationLengthMin == 10)
    
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

    def queryTypeStddev(results: Seq[Result] = goodResults.toSeq) = 
      results.groupBy(_.queryDesc).map {
        case (queryDesc, results) => (queryDesc, results.map(_.responseTimes)
						  .reduceLeft(_ + _)
						  .map(_.stddev))
      }
      
    def queryTypeQuantileAllHistograms(results: Seq[Result] = goodResults.toSeq, quantile: Double = 0.90) = 
      results.groupBy(_.queryDesc).map {
        case (queryDesc, results) => (queryDesc, results.map(_.responseTimes)
                                                        .map(_.quantile(quantile))
                                                        .foldLeft(Histogram(1,1000))(_ add _))
      }

    def queryTypeQuantileAllHistogramsMedian(results: Seq[Result] = goodResults.toSeq, quantile: Double = 0.90) = {
      queryTypeQuantileAllHistograms(results, quantile).map {
        case (queryDesc, hist) => (queryDesc, hist.median)
      }
    }
    
    def queryTypeQuantileAllHistogramsStddev(results: Seq[Result] = goodResults.toSeq, quantile: Double = 0.90) = {
      queryTypeQuantileAllHistograms(results, quantile).map {
        case (queryDesc, hist) => (queryDesc, hist.stddev)
      }
    }

    def thoughtstreamGraph =
      queryTypeQuantile().filter(_._1.queryName == "thoughtstream")
    
		def quantileCsv(results: Seq[Result] = goodResults.toSeq, quantile: Double = 0.90, queryName: String) = {
		  val quantiles = queryTypeQuantile(results, quantile).filter(_._1.queryName == queryName)
      dataCsv(quantiles)
		}
		
		def stddevCsv(results: Seq[Result] = goodResults.toSeq, queryName: String) = {
		  val stddev = queryTypeStddev(results).filter(_._1.queryName == queryName)
		  dataCsv(stddev)
		}
		
		def dataCsv(data:Map[QueryDescription, Any]) = {
		  data.map(i => {
		    var line:List[String] = Nil
		    line = i._1.queryName :: line
		    
		    (1 to i._1.parameters.length).foreach(j =>
		      line = i._1.parameters(j-1).toString :: line
		    )
		    
		    val num = i._2 match {
		      case a: Option[Any] => a.get
		      case b: Any => b
		      case _ =>
		    }
		    line = num.toString :: line
		    println(line.reverse.mkString(","))
		  })
		}
		
		def thoughtstreamQuantileCsv(results: Seq[Result] = goodResults.toSeq, quantile: Double = 0.90) = {
		  println("queryName,numSubs,numPerPage,latency")
		  quantileCsv(results, quantile, "thoughtstream")
		}
		
		def myThoughtsQuantileCsv(results: Seq[Result] = goodResults.toSeq, quantile: Double = 0.90) = {
		  println("queryName,numPerPage,latency")
		  quantileCsv(results, quantile, "myThoughts")
		}

		def usersFollowedByQuantileCsv(results: Seq[Result] = goodResults.toSeq, quantile: Double = 0.90) = {
		  println("queryName,numSubs,latency")
		  quantileCsv(results, quantile, "usersFollowedBy")
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

    def benchmarkScadr(cluster: ScadsCluster = defaultScadr.newCluster) = {
      defaultRunner.schedule(cluster,resultsCluster)
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
