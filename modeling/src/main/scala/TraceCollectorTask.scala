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

case class TraceCollectorTask(
	var clusterAddress: String, 
	var baseCardinality: Int, 
	var warmupLengthInMinutes: Int = 5, 
	var numStorageNodes: Int = 1, 
	var numQueriesPerCardinality: Int = 1000, 
	var sleepDurationInMs: Int = 100
) extends AvroTask with AvroRecord {
	var beginningOfCurrentWindow = 0.toLong

	def run(): Unit = {
		println("made it to run function")
		val clusterRoot = ZooKeeperNode(clusterAddress)
    val cluster = new ExperimentalScadsCluster(clusterRoot)

    logger.info("Adding servers to cluster for each namespace")
    cluster.blockUntilReady(numStorageNodes)

    /* get namespaces */
    val ns = cluster.getNamespace[PrefixedNamespace]("prefixedNamespace")

    /* create executor that records trace to fileSink */
    val fileSink = new FileTraceSink(new File("/mnt/piqltrace.avro"))
    implicit val executor = new ParallelExecutor with TracingExecutor {
      val sink = fileSink
    }

    /* Register a listener that will record all messages sent/recv to fileSink */
    val messageTracer = new MessagePassingTracer(fileSink)
    MessageHandler.registerListener(messageTracer)

    /* Bulk load some test data into the namespaces */
    //ns ++= (1 to 10).view.flatMap(i => (1 to 10000).map(j => PrefixedNamespace(i,j)))
		ns ++= (1 to 10).view.flatMap(i => (1 to 20).map(j => PrefixedNamespace(i,j)))

    /**
     * Write queries against relations and create optimized function using .toPiql
     * toPiql uses implicit executor defined above to run queries
     */
		//val rangeSizes = List(10,50,100,500,1000)
		val rangeSizes = List(5,10,15)
		val getRangeQueries = rangeSizes.map(currentRangeSize => ns.where("f1".a === 1).limit(currentRangeSize).toPiql("getRangeQuery-rangeLength=" + currentRangeSize.toString))

		// initialize window
		beginningOfCurrentWindow = System.nanoTime
				    
		// warmup to avoid JITing effects
		fileSink.recordEvent(WarmupEvent(warmupLengthInMinutes, true))
		while (withinWarmup) {
			(1 to 10).foreach(i => {
	      fileSink.recordEvent(QueryEvent("getRangeQuery" + i, true))
	      getRangeQueries(0)()
	      fileSink.recordEvent(QueryEvent("getRangeQuery" + i, false))
	
				//Thread.sleep(100)
	    })
		}
		fileSink.recordEvent(WarmupEvent(warmupLengthInMinutes, false))
				

    /* Run some queries */
		rangeSizes.indices.foreach(r => {
			fileSink.recordEvent(ChangeRangeLengthEvent(rangeSizes(r)))
			
			//(1 to 1000).foreach(i => {
			(1 to 2).foreach(i => {
	      fileSink.recordEvent(QueryEvent("getRangeQuery" + i, true))
	      getRangeQueries(r)()
	      fileSink.recordEvent(QueryEvent("getRangeQuery" + i, false))

				//Thread.sleep(100)
	    })
		})

    //Flush trace messages to the file
    fileSink.flush()

		// Write IP to ZooKeeper
		clusterRoot.data = java.net.InetAddress.getLocalHost.getHostName.getBytes
		TraceS3Cache.uploadFile("/mnt/piqltrace.avro")
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
		((baseCardinality*0.5).toInt :: (baseCardinality*0.75).toInt :: baseCardinality :: baseCardinality*2 :: baseCardinality*10 :: Nil)
	}
	
}
