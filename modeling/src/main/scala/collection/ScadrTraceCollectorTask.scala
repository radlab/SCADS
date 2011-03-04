package edu.berkeley.cs
package scads
package piql
package modeling

import deploylib.mesos._
import deploylib.ec2._
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

case class ScadrTraceCollectorTask(
  var params: RunParams
) extends AvroTask with AvroRecord {
  var beginningOfCurrentWindow = 0.toLong
  
  def run(): Unit = {
    val logger = Logger()

    logger.info("made it to run function")
    
    /* set up cluster */
    val clusterRoot = ZooKeeperNode(params.clusterParams.clusterAddress)
    val cluster = new ExperimentalScadsCluster(clusterRoot)

    logger.info("Adding servers...")
    cluster.blockUntilReady(params.clusterParams.numStorageNodes)

    /* create executor that records trace to fileSink */
    logger.info("creating executor...")
    val fileSink = new FileTraceSink(new File("/mnt/piqltrace.avro"))
    implicit val executor = new ParallelExecutor with TracingExecutor {
      val sink = fileSink
    }

    /* Register a listener that will record all messages sent/recv to fileSink */
    logger.info("registering listener...")
    val messageTracer = new MessagePassingTracer(fileSink)
    MessageHandler.registerListener(messageTracer)

    // TODO:  make this work for either generic or scadr
    val queryRunner = new ScadrQuerySpecRunner(params)
    queryRunner.setupNamespacesAndCreateQuery(cluster)

    // initialize window
    beginningOfCurrentWindow = System.nanoTime
            
    // warmup to avoid JITing effects
    // TODO:  move this to a function
    logger.info("beginning warmup...")
    fileSink.recordEvent(WarmupEvent(params.warmupLengthInMinutes, true))
    var queryCounter = 1
    //val cardinalityList = params.clusterParams.getCardinalityList(params.queryType)
    val cardinalityList = List(100,250,500) // numSubscriptions
    
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
    logger.info("beginning run...")
    cardinalityList.indices.foreach(r => {
      logger.info("current cardinality = " + cardinalityList(r).toString)
      fileSink.recordEvent(ChangeCardinalityEvent(cardinalityList(r)))
      
      (1 to params.numQueriesPerCardinality).foreach(i => {
        fileSink.recordEvent(QueryEvent(params.queryType + i + "-" + cardinalityList(r), true))

        queryRunner.callQuery(cardinalityList(r))

        fileSink.recordEvent(QueryEvent(params.queryType + i + "-" + cardinalityList(r), false))
        Thread.sleep(params.sleepDurationInMs)
      })
    })

    // TODO:  put all of this cleanup in a function
    //Flush trace messages to the file
    logger.info("flushing messages to file...")
    fileSink.flush()

    // Upload file to S3
    val currentTimeString = System.currentTimeMillis().toString
    
    logger.info("uploading data...")
    TraceS3Cache.uploadFile("/mnt/piqltrace.avro", currentTimeString)
    
    // Publish to SNSClient
    ExperimentNotification.completions.publish("experiment completed at " + currentTimeString, params.toString)
    
    logger.info("Finished with trace collection.")
  }
  
  def convertMinutesToNanoseconds(minutes: Int): Long = {
    minutes.toLong * 60.toLong * 1000000000.toLong
  }

  def withinWarmup: Boolean = {
    val currentTime = System.nanoTime
    currentTime < beginningOfCurrentWindow + convertMinutesToNanoseconds(params.warmupLengthInMinutes)
  }
}
