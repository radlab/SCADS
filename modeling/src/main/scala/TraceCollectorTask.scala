package edu.berkeley.cs
package scads
package piql
package modeling

import deploylib.mesos._
import comm._
import storage._
import piql._
import perf._
import avro.marker._
import avro.runtime._

import net.lag.logging.Logger
import java.io.File
import java.net._

import scala.collection.JavaConversions._
import scala.collection.mutable._

import com.amazonaws.services.sns._
import com.amazonaws.auth._
import com.amazonaws.services.sns.model._

case class TraceCollectorTask(
  var clusterAddress: String, 
  var queryType: String,
  var baseCardinality: Int, 
  var warmupLengthInMinutes: Int = 5, 
  var numStorageNodes: Int = 1, 
  var numQueriesPerCardinality: Int = 1000, 
  var sleepDurationInMs: Int = 100
) extends AvroTask with AvroRecord {
  import TraceCollectorTask._

  var beginningOfCurrentWindow = 0.toLong
  var lowerBound = 10
  
  def run(): Unit = {
    println("made it to run function")
    val clusterRoot = ZooKeeperNode(clusterAddress)
    val cluster = new ExperimentalScadsCluster(clusterRoot)

    logger.info("Adding servers to cluster for each namespace")
    cluster.blockUntilReady(numStorageNodes)

    /* get namespaces */
    println("getting namespace...")
    val ns = cluster.getNamespace[PrefixedNamespace]("prefixedNamespace")
    /*
    val r1 = cluster.getNamespace[R1]("r1")
    val r2 = cluster.getNamespace[R2]("r2")
    */

    /* create executor that records trace to fileSink */
    println("creating executor...")
    val fileSink = new FileTraceSink(new File("/mnt/piqltrace.avro"))
    implicit val executor = new ParallelExecutor with TracingExecutor {
      val sink = fileSink
    }

    /* Register a listener that will record all messages sent/recv to fileSink */
    println("registering listener...")
    val messageTracer = new MessagePassingTracer(fileSink)
    MessageHandler.registerListener(messageTracer)

    /* Bulk load some test data into the namespaces */
    println("loading data...")
    ns ++= (1 to 10).view.flatMap(i => (1 to getNumDataItems).map(j => PrefixedNamespace(i,j)))   // might want to fix hard-coded 10 at some point
    /*
    r1 ++= (1 to getNumDataItems).view.map(i => R1(i))
    r2 ++= (1 to 10).view.flatMap(i => (1 to getNumDataItems).map(j => R2(i,j)))    
    */

    /**
     * Write queries against relations and create optimized function using .toPiql
     * toPiql uses implicit executor defined above to run queries
     */
    println("creating queries...")
    val cardinalityList = getCardinalityList
    
    val queries = cardinalityList.map(currentCardinality => 
      ns.where("f1".a === 1)
          .limit(currentCardinality)
          .toPiql("getRangeQuery-rangeLength=" + currentCardinality.toString)
    )
    /*
    val queries = cardinalityList.map(currentCardinality => 
      r2.where("f1".a === 1)
          .limit(currentCardinality)
          .join(r1)
          .where("r1.f1".a === "r2.f2".a)
          .toPiql("joinQuery-cardinality=" + currentCardinality.toString)
    )
    */
    

    // initialize window
    beginningOfCurrentWindow = System.nanoTime
            
    // warmup to avoid JITing effects
    println("beginning warmup...")
    fileSink.recordEvent(WarmupEvent(warmupLengthInMinutes, true))
    var queryCounter = 1
    while (withinWarmup) {
      cardinalityList.indices.foreach(r => {
        fileSink.recordEvent(ChangeCardinalityEvent(cardinalityList(r)))
        
        fileSink.recordEvent(QueryEvent("getRangeQuery" + queryCounter, true))
        //fileSink.recordEvent(QueryEvent("joinQuery" + queryCounter, true))
        
        val resLengthWarmup = queries(r)().length
        if (resLengthWarmup != cardinalityList(r))
          throw new RuntimeException("expected cardinality: " + cardinalityList(r).toString + ", got: " + resLengthWarmup.toString)
        
        fileSink.recordEvent(QueryEvent("getRangeQuery" + queryCounter, false))
        //fileSink.recordEvent(QueryEvent("joinQuery" + queryCounter, false))
  
        Thread.sleep(sleepDurationInMs)
        queryCounter += 1
      })
    }
    fileSink.recordEvent(WarmupEvent(warmupLengthInMinutes, false))
        

    /* Run some queries */
    println("beginning run...")
    cardinalityList.indices.foreach(r => {
      println("current cardinality = " + cardinalityList(r).toString)
      fileSink.recordEvent(ChangeCardinalityEvent(cardinalityList(r)))
      
      (1 to numQueriesPerCardinality).foreach(i => {
        fileSink.recordEvent(QueryEvent("getRangeQuery" + i, true))
        //fileSink.recordEvent(QueryEvent("joinQuery" + i, true))
  
        val resLength = queries(r)().length
        if (resLength != cardinalityList(r))
          throw new RuntimeException("expected cardinality: " + cardinalityList(r).toString + ", got: " + resLength.toString)
  
        fileSink.recordEvent(QueryEvent("getRangeQuery" + i, false))
        //fileSink.recordEvent(QueryEvent("joinQuery" + i, false))

        Thread.sleep(sleepDurationInMs)
      })
    })

    //Flush trace messages to the file
    println("flushing messages to file...")
    fileSink.flush()

    // Upload file to S3
    println("uploading data...")
    TraceS3Cache.uploadFile("/mnt/piqltrace.avro")
    
    // Publish to SNSClient
    snsClient.publishToTopic(topicArn, getExperimentDescription, "experiment completed at " + System.currentTimeMillis())
    
    println("Finished with trace collection.")
  }

  def setupNamespacesAndCreateQuery(cluster: ExperimentalScadsCluster)(implicit executor: QueryExecutor):OptimizedQuery = {
    val query = queryType match {
      case "getQuery" => 
        val r1 = cluster.getNamespace[R1]("r1")
        r1 ++= (1 to 10).view.map(i => R1(i))

        r1.where("f1".a === 1).toPiql("getQuery")
      case "getRangeQuery" =>
        val r2 = cluster.getNamespace[R2]("r2")
        r2 ++= (1 to 10).view.flatMap(i => (1 to getNumDataItems).map(j => R2(i,j)))    

        r2.where("f1".a === 1)
            .limit(0.?, getMaxCardinality)
            .toPiql("getRangeQuery")      
      case "lookupJoinQuery" =>
        val r1 = cluster.getNamespace[R1]("r1")
        val r2 = cluster.getNamespace[R2]("r2")
        r1 ++= (1 to getNumDataItems).view.map(i => R1(i))
        r2 ++= (1 to 10).view.flatMap(i => (1 to getNumDataItems).map(j => R2(i,j)))    
        
        r2.where("f1".a === 1)
            .limit(0.?, getMaxCardinality)
            .join(r1)
            .where("r1.f1".a === "r2.f2".a)
            .toPiql("joinQuery")
      case "mergeSortJoinQuery" =>
        val r1 = cluster.getNamespace[R1]("r1")
        val r2 = cluster.getNamespace[R2]("r2")
        val r2Prime = cluster.getNamespace[R2]("r2Prime")
  
        r1 ++= (1 to 10).view.map(i => R1(i))
        r2 ++= (1 to 10).view.flatMap(i => (1 to getNumDataItems).map(j => R2(i,j)))    
        r2Prime ++= (1 to 10).view.flatMap(i => (1 to getNumDataItems).map(j => R2(i,j)))    
        
        // what to do about these 2 limits?
        r2.where("f1".a === 1)
              .limit(5)
              .join(r2Prime)
              .where("r2.f2".a === "r2Prime.f1".a)
              .sort("r2Prime.f2".a :: Nil)
              .limit(10)
              .toPiql("mergeSortJoinQuery")
    }
    query
  }
  
  def convertMinutesToNanoseconds(minutes: Int): Long = {
    minutes.toLong * 60.toLong * 1000000000.toLong
  }

  def withinWarmup: Boolean = {
    val currentTime = System.nanoTime
    currentTime < beginningOfCurrentWindow + convertMinutesToNanoseconds(warmupLengthInMinutes)
  }
  
  def getNumDataItems: Int = {
    getMaxCardinality*10
  }
  
  def getMaxCardinality: Int = {
    getCardinalityList.sortWith(_ > _).head
  }
  
  def getCardinalityList: List[Int] = {
    ((baseCardinality*0.5).toInt :: (baseCardinality*0.75).toInt :: baseCardinality :: baseCardinality*2 :: baseCardinality*10 :: baseCardinality*100:: Nil)
  }

  def getExperimentDescription: String = {
    "experiment had the following params:\n" + getExperimentParamString
  }
  
  def getExperimentParamString: String = {
    List(
      "clusterAddress: " + clusterAddress.toString,
      "queryType: " + queryType.toString,
      "baseCardinality: " + baseCardinality.toString,
      "warmupLengthInMinutes: " + warmupLengthInMinutes.toString,
      "numStorageNodes: " + numStorageNodes.toString,
      "numQueriesPerCardinality: " + numQueriesPerCardinality.toString,
      "sleepDurationInMs: " + sleepDurationInMs.toString
    ).mkString("\n")
  }
}

object TraceCollectorTask {
  val snsClient = new SimpleAmazonSNSClient
  val topicArn = snsClient.createOrRetrieveTopicAndReturnTopicArn("experimentCompletion")
  snsClient.subscribeViaEmail(topicArn, "kristal.curtis@gmail.com")
}

class SimpleAmazonSNSClient {
  val snsClient = createAmazonSNSClient
  
  def createAmazonSNSClient:AmazonSNSClient = {
    return new AmazonSNSClient(new BasicAWSCredentials("AKIAILGVHXBVDZKFJZQQ", "VdB4xNttSvG8DOeF90XQI4jqg6EOi6L00nt0Lq3n"))
  }
  
  // ARN = Amazon Resource Name
  def createTopicAndReturnTopicArn(topicText: String):String = {
    val res = snsClient.createTopic(new CreateTopicRequest(topicText))
    res.getTopicArn
  }
  
  def createOrRetrieveTopicAndReturnTopicArn(topicText: String):String = {
    val topics:Buffer[Topic] = snsClient.listTopics.getTopics
    val topicsList:List[Topic] = topics.toList
    val matchingTopics:List[Topic] = topicsList.filter(t => t.getTopicArn.contains(topicText))
    val topicArn = matchingTopics.length match {
      case 0 => createTopicAndReturnTopicArn(topicText)
      case 1 => matchingTopics.head.getTopicArn
      case 2 => 
        throw new TopicException
        matchingTopics.head.getTopicArn
    }
    topicArn
  }
  
  def subscribeViaEmail(topicArn: String, emailAddress: String) = {
    snsClient.subscribe(new SubscribeRequest(topicArn, "email", emailAddress))
  }
  
  def publishToTopic(topicArn: String, message: String, subject: String) = {
    snsClient.publish(new PublishRequest(topicArn, message, subject))
  }
}

case class TopicException() extends Exception