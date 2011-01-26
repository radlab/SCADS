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
		println("getting namespace...")
    val ns = cluster.getNamespace[PrefixedNamespace]("prefixedNamespace")

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
		ns ++= (1 to 10).view.flatMap(i => (1 to getNumDataItems).map(j => PrefixedNamespace(i,j)))	 // might want to fix hard-coded 10 at some point

    /**
     * Write queries against relations and create optimized function using .toPiql
     * toPiql uses implicit executor defined above to run queries
     */
		println("creating queries...")
		val cardinalityList = getCardinalityList
		val getRangeQueries = cardinalityList.map(currentCardinality => ns.where("f1".a === 1).limit(currentCardinality).toPiql("getRangeQuery-rangeLength=" + currentCardinality.toString))

		// initialize window
		beginningOfCurrentWindow = System.nanoTime
				    
		// warmup to avoid JITing effects
		println("beginning warmup...")
		fileSink.recordEvent(WarmupEvent(warmupLengthInMinutes, true))
		var queryCounter = 1
		while (withinWarmup) {
			cardinalityList.indices.foreach(i => {
	      fileSink.recordEvent(QueryEvent("getRangeQuery" + queryCounter, true))
	      getRangeQueries(i)()
	      fileSink.recordEvent(QueryEvent("getRangeQuery" + queryCounter, false))
	
				Thread.sleep(sleepDurationInMs)
				queryCounter += 1
	    })
		}
		fileSink.recordEvent(WarmupEvent(warmupLengthInMinutes, false))
				

    /* Run some queries */
		println("beginning run...")
		cardinalityList.indices.foreach(r => {
			println("current cardinality = " + cardinalityList(r).toString)
			fileSink.recordEvent(ChangeRangeLengthEvent(cardinalityList(r)))
			
			(1 to numQueriesPerCardinality).foreach(i => {
	      fileSink.recordEvent(QueryEvent("getRangeQuery" + i, true))
	      getRangeQueries(r)()
	      fileSink.recordEvent(QueryEvent("getRangeQuery" + i, false))

				Thread.sleep(sleepDurationInMs)
	    })
		})

    //Flush trace messages to the file
		println("flusing messages to file...")
    fileSink.flush()

		// Write IP to ZooKeeper
		clusterRoot.data = java.net.InetAddress.getLocalHost.getHostName.getBytes
		
		// Upload file to S3
		println("uploading data...")
		TraceS3Cache.uploadFile("/mnt/piqltrace.avro")
		
		println("Finished with trace collection.")
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
