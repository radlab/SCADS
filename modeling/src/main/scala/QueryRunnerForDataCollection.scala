package edu.berkeley.cs
package scads
package piql
package modeling

import comm._
import storage._
import piql._
import avro.marker._
import avro.runtime._

import net.lag.logging.Logger

import java.io.File


case class PrefixedNamespace(var f1: Int, var f2: Int) extends AvroPair {
  var v = 1 //HACK: Required due to storage engine bug
}

object QueryRunnerForDataCollection extends optional.Application {
  val logger = Logger()
	//var warmupLengthInMinutes = 0
	var beginningOfCurrentWindow = 0.toLong

  //def main(args: Array[String]): Unit = {
	def main(clusterAddress: Option[String], warmupLengthInMinutes: Int = 5): Unit = {
		def withinWarmup: Boolean = {
			val currentTime = System.nanoTime
			currentTime < beginningOfCurrentWindow + convertMinutesToNanoseconds(warmupLengthInMinutes)
		}
	
    /* Connect to scads cluster */
		val cluster = clusterAddress.map(p => new ScadsCluster(ZooKeeperNode(p))).getOrElse(TestScalaEngine.newScadsCluster())

    /* get namespaces */
    val ns = cluster.getNamespace[PrefixedNamespace]("prefixedNamespace")

    /* create executor that records trace to fileSink */
    val fileSink = new FileTraceSink(new File("piqltrace.avro"))
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
		val getRangeQueries = rangeSizes.map(currentRangeSize => ns.where("f1".a === 1).limit(currentRangeSize).toPiql)

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

    System.exit(0)
  }

	def convertMinutesToNanoseconds(minutes: Int): Long = {
		minutes.toLong * 60.toLong * 1000000000.toLong
	}
}
