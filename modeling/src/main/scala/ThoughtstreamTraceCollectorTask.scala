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

case class ThoughtstreamTraceCollectorTask(
  var params: RunParams
) extends AvroTask with AvroRecord {
  import ThoughtstreamTraceCollectorTask._

  var beginningOfCurrentWindow = 0.toLong
  
  def run(): Unit = {
    /* set up cluster */
    println("setting up cluster...")
    val clusterRoot = ZooKeeperNode(params.clusterParams.clusterAddress)
    val cluster = new ExperimentalScadsCluster(clusterRoot)
    cluster.blockUntilReady(params.clusterParams.numStorageNodes)

    /* create executor that records trace to fileSink */
    println("creating executor...")
    val fileSink = new FileTraceSink(new File("/mnt/piqltrace.avro"))
    //implicit val executor = new ParallelExecutor with TracingExecutor {
    implicit val executor = new LocalUserExecutor with TracingExecutor {
      val sink = fileSink
    }

    /* Register a listener that will record all messages sent/recv to fileSink */
    println("registering listener...")
    val messageTracer = new MessagePassingTracer(fileSink)
    MessageHandler.registerListener(messageTracer)

    // TODO:  make this work for either generic or scadr
    val queryRunner = new ScadrQuerySpecRunner(params)
    queryRunner.setupNamespacesAndCreateQuery(cluster)  // creates thoughtstream query

    // initialize window
    beginningOfCurrentWindow = System.nanoTime
            
    // set up thoughtstream-specific run params
    val numSubscriptionsPerUserList = List(100,300,500)
    val numPerPageList = List(10,30,50)
    //val numSubscriptionsPerUserList = List(100)
    //val numPerPageList = List(10)
    
    // warmup to avoid JITing effects
    // TODO:  move this to a function
    println("beginning warmup...")
    fileSink.recordEvent(WarmupEvent(params.warmupLengthInMinutes, true))
    var queryCounter = 1
    
    while (withinWarmup) {
      fileSink.recordEvent(QueryEvent(params.queryType + queryCounter, true))

      queryRunner.callThoughtstream(numSubscriptionsPerUserList.head, numPerPageList.head)

      fileSink.recordEvent(QueryEvent(params.queryType + queryCounter, false))
      Thread.sleep(params.sleepDurationInMs)
      queryCounter += 1
    }
    fileSink.recordEvent(WarmupEvent(params.warmupLengthInMinutes, false))
        

    /* Run some queries */
    // TODO:  move this to a function
    println("beginning run...")
    
    numSubscriptionsPerUserList.foreach(numSubs => {
      fileSink.recordEvent(ChangeNamedCardinalityEvent("numSubscriptions", numSubs))
      println("numSubscriptions = " + numSubs)

      numPerPageList.foreach(numPerPage => {
        println("numPerPage = " + numPerPage)
        fileSink.recordEvent(ChangeNamedCardinalityEvent("numPerPage", numPerPage))
        
        (1 to params.numQueriesPerCardinality).foreach(i => {
          fileSink.recordEvent(QueryEvent(params.queryType + i + "-" + numSubs + "-" + numPerPage, true))

          queryRunner.callThoughtstream(numSubs, numPerPage)

          fileSink.recordEvent(QueryEvent(params.queryType + i + "-" + numSubs + "-" + numPerPage, false))
          Thread.sleep(params.sleepDurationInMs)
        })
      })
    })
    
    // TODO:  put all of this cleanup in a function
    //Flush trace messages to the file
    println("flushing messages to file...")
    fileSink.flush()

    // Upload file to S3
    val currentTimeString = System.currentTimeMillis().toString
    
    println("uploading data...")
    TraceS3Cache.uploadFile("/mnt/piqltrace.avro", currentTimeString)
    
    // Publish to SNSClient
    snsClient.publishToTopic(topicArn, params.toString, "experiment completed at " + currentTimeString)
    
    println("Finished with trace collection.")
  }
  
  def convertMinutesToNanoseconds(minutes: Int): Long = {
    minutes.toLong * 60.toLong * 1000000000.toLong
  }

  def withinWarmup: Boolean = {
    val currentTime = System.nanoTime
    currentTime < beginningOfCurrentWindow + convertMinutesToNanoseconds(params.warmupLengthInMinutes)
  }
}

object ThoughtstreamTraceCollectorTask {
  // for email notifications
  val snsClient = new SimpleAmazonSNSClient
  val topicArn = snsClient.createOrRetrieveTopicAndReturnTopicArn("experimentCompletion")
  snsClient.subscribeViaEmail(topicArn, "kristal.curtis@gmail.com")
}

