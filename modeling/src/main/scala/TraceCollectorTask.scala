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
  var params: RunParams
) extends AvroTask with AvroRecord {
  import TraceCollectorTask._

  var beginningOfCurrentWindow = 0.toLong
  
  def run(): Unit = {
    println("made it to run function")
    val clusterRoot = ZooKeeperNode(params.clusterAddress)
    val cluster = new ExperimentalScadsCluster(clusterRoot)

    logger.info("Adding servers to cluster for each namespace")
    cluster.blockUntilReady(params.numStorageNodes)

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

    /* Get namespaces */
    /* Bulk load some test data into the namespaces */
    /**
     * Write queries against relations and create optimized function using .toPiql
     * toPiql uses implicit executor defined above to run queries
     */
    val queryRunner = new GenericQuerySpecRunner(params)
    queryRunner.setupNamespacesAndCreateQuery(cluster)

    // initialize window
    beginningOfCurrentWindow = System.nanoTime
            
    // warmup to avoid JITing effects
    // TODO:  move this to a function
    println("beginning warmup...")
    fileSink.recordEvent(WarmupEvent(params.warmupLengthInMinutes, true))
    var queryCounter = 1
    val cardinalityList = params.getCardinalityList
    
    while (withinWarmup) {
      fileSink.recordEvent(QueryEvent(params.queryType + queryCounter, true))

      queryRunner.callQuery(cardinalityList.head)

      fileSink.recordEvent(QueryEvent(params.queryType + queryCounter, false))
      Thread.sleep(params.sleepDurationInMs)
      queryCounter += 1
    }
    fileSink.recordEvent(WarmupEvent(params.warmupLengthInMinutes, false))
        

    /* Run some queries */
    // TODO:  move this to a function
    println("beginning run...")
    cardinalityList.indices.foreach(r => {
      println("current cardinality = " + cardinalityList(r).toString)
      fileSink.recordEvent(ChangeCardinalityEvent(cardinalityList(r)))
      
      (1 to params.numQueriesPerCardinality).foreach(i => {
        fileSink.recordEvent(QueryEvent(params.queryType + i, true))

        queryRunner.callQuery(cardinalityList(r))

        fileSink.recordEvent(QueryEvent(params.queryType + i, false))
        Thread.sleep(params.sleepDurationInMs)
      })
    })

    // TODO:  put all of this cleanup in a function
    //Flush trace messages to the file
    println("flushing messages to file...")
    fileSink.flush()

    // Upload file to S3
    println("uploading data...")
    TraceS3Cache.uploadFile("/mnt/piqltrace.avro")
    
    // Publish to SNSClient
    snsClient.publishToTopic(topicArn, params.toString, "experiment completed at " + System.currentTimeMillis())
    
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

object TraceCollectorTask {
  // for email notifications
  val snsClient = new SimpleAmazonSNSClient
  val topicArn = snsClient.createOrRetrieveTopicAndReturnTopicArn("experimentCompletion")
  snsClient.subscribeViaEmail(topicArn, "kristal.curtis@gmail.com")
}

