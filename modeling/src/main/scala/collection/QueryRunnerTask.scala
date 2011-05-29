package edu.berkeley.cs
package scads
package piql
package modeling

import avro.marker._
import avro.runtime._
import storage._
import perf._
import comm._
import deploylib._

import java.io.File
import scala.util.Random
import java.util.concurrent._
import java.util.concurrent.atomic._
import collection.JavaConversions._

abstract class ParameterGenerator {
  def getValue(rand: Random): (Any, Option[Int])
}

case class CardinalityList(values: IndexedSeq[Int]) extends ParameterGenerator {
  def getValue(rand: Random) = {
    val cardinality = values(rand.nextInt(values.size))
    (cardinality, Some(cardinality))
  }
}

case class QuerySpec(query: OptimizedQuery, paramGenerators: Seq[ParameterGenerator])


case class QueryDescription(var queryName: String, var parameters: Seq[Int], var resultCardinality: Int) extends AvroRecord

case class Result(var queryDesc: QueryDescription, var hostname: String, var timestamp: Long) extends AvroPair {
  var iteration: Int = _
  var clientConfig: QueryRunnerTask = null
  var responseTimes: Histogram = null
  var failedQueries: Int = 0
}	

abstract class QueryProvider {
  def getQueryList(cluster: ScadsCluster, executor: QueryExecutor): IndexedSeq[QuerySpec]
}

//TODO: Record cluster loader configuration
//TODO: Make executor variable
case class QueryRunnerTask(var numClients: Int,
			   var queryProvider: String,
			   var traceIterators: Boolean = true,
			   var traceMessages: Boolean = true,
			   var traceQueries: Boolean = true,
			   var iterations: Int = 6,
			   var iterationLengthMin: Int = 10,
			   var threads: Int = 5)
 extends AvroRecord with ReplicatedExperimentTask {
  var clusterAddress: String = _
  var resultClusterAddress: String = _
  var experimentAddress: String = _

  def run(): Unit = {
    val hostname = java.net.InetAddress.getLocalHost.getHostName

    logger.info("Query Runner Task Starting")
    val results = new ScadsCluster(ZooKeeperNode(resultClusterAddress)).getNamespace[Result]("queryRunnerResults")

    val experimentRoot = ZooKeeperNode(experimentAddress)
    val coordination = experimentRoot.getOrCreate("coordination/clients")
    val clientId = coordination.registerAndAwait("clientStart", numClients, timeout=24*60*60*1000)  // 1 day

    val clusterRoot = ZooKeeperNode(clusterAddress)
    clusterRoot.awaitChild("clusterReady", timeout=24*60*60*1000) // 1 day
    val cluster = new ScadsCluster(clusterRoot)

    val traceFile = new File("piqltrace.avro")
    val traceSink = new FileTraceSink(traceFile)

    val executor =
      if(traceIterators)
	      new ParallelExecutor with TracingExecutor {
	        val sink = traceSink
	      }
      else
	      new ParallelExecutor
    
    if(traceMessages) {
      logger.info("registering listener...")
      val messageTracer = new MessagePassingTracer(traceSink)
      MessageHandler.registerListener(messageTracer)
    }

    val querySpecs = Class.forName(queryProvider).newInstance.asInstanceOf[QueryProvider].getQueryList(cluster, executor)

    coordination.registerAndAwait("startQueryRunning", numClients)
    for(iteration <- (1 to iterations)) {
      //coordination.registerAndAwait("startIteration" + iteration, numClients)
      val responseTimes = new ConcurrentHashMap[QueryDescription, Histogram]
      val failedQueries = new ConcurrentHashMap[QueryDescription, AtomicInteger]

      logger.info("Beginning iteration %d", iteration)
      if(MessageHandler.registrySize > 0) {
        logger.warning("Registered Actor Count: %d. Reseting", MessageHandler.registrySize)
        MessageHandler.reset()
      }
      (1 to threads).pmap(threadId => {
	      val seed = java.net.InetAddress.getLocalHost.getHostName + System.currentTimeMillis + threadId
	      val rand = new Random(seed.hashCode)
	      val runTime = iterationLengthMin * 60 * 1000L
	      val iterationStartTime = getTime
	      var queryCounter = 0

	      while(getTime - iterationStartTime < runTime) {
	        val querySpec = querySpecs(rand.nextInt(querySpecs.size))
	        val params = querySpec.paramGenerators.map(_.getValue(rand))
	        val runParams = params.map(_._1)
	        val paramsDesc = params.flatMap(_._2)
	        val queryDesc = QueryDescription(querySpec.query.name.getOrElse("unnamed"), paramsDesc, 0)

	        if(traceQueries)
	          traceSink.recordEvent(QueryEvent(queryDesc.queryName, paramsDesc, queryCounter, true))

	        val startTime = getTime
	        try {
	          val results = querySpec.query.apply(runParams: _*)
	          queryDesc.resultCardinality = results.size
	          val endTime = getTime

	          if(traceQueries)
	            traceSink.recordEvent(QueryEvent(queryDesc.queryName, paramsDesc, queryCounter, false))

	          if(!responseTimes.contains(queryDesc)) {
	            responseTimes.putIfAbsent(queryDesc, Histogram(1, 1000))
	          }

	          responseTimes.get(queryDesc) += (endTime - startTime)
	          queryCounter += 1
	        } catch {
	          case e =>
	            logger.warning(e, "Query %s failed", querySpec.query.name)
	            if(!failedQueries.contains(queryDesc))
		            failedQueries.putIfAbsent(queryDesc, new AtomicInteger)
	            failedQueries.get(queryDesc).incrementAndGet
	        }
	      }
	
	      logger.info("Thread %d completed with %d queries.", threadId, queryCounter)
      })

      logger.info("Recording results to scads")
      results ++= responseTimes.entrySet.map(e => {
	      val result = Result(e.getKey, hostname, System.currentTimeMillis)
	      result.iteration = iteration
	      result.clientConfig = this
	      result.responseTimes = e.getValue
	      result.failedQueries = Option(failedQueries.get(e.getKey)).map(_.get).getOrElse(0)
	      result
      })
    }

    //Upload traces to S3
    if (traceQueries || traceIterators || traceMessages) {
      traceSink.flush()
      TraceS3Cache.uploadFile(traceFile, List(queryProvider, threads, iterations*iterationLengthMin, experimentRoot.name).mkString("/"), "client" + clientId)
    }
  }

  @inline protected final def getTime = System.nanoTime / 1000000
}
