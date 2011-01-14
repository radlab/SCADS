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

object QueryRunnerForDataCollection {
  val logger = Logger()
	val windowLengthInMinutes = 5
	var beginningOfCurrentWindow = 0.toLong

  def main(args: Array[String]): Unit = {
    /* Connect to scads cluster */
    val cluster = args.size match {
      case 0 => TestScalaEngine.newScadsCluster()
      case 1 => {
				println(args(0))
				new ScadsCluster(ZooKeeperNode(args(0)))
			}
      case _ => {
				println("Usage: [cluster address]")
				System.exit(1)
				return
      }
    }

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
    ns ++= (1 to 10).view.flatMap(i => (1 to 10000).map(j => PrefixedNamespace(i,j)))

    /**
     * Write queries against relations and create optimized function using .toPiql
     * toPiql uses implicit executor defined above to run queries
     */
    val getRangeQuery = ns.where("f1".a === 1)
			  .limit(10).toPiql
			// change "10" to "0.?", then pass in limit when call getRangeQuery

		// initialize window
		beginningOfCurrentWindow = System.nanoTime
				    
		// warmup to avoid JITing effects
		// can I omit the logging?
		fileSink.recordEvent(WarmupEvent(windowLengthInMinutes, true))
		while (withinWindow) {
			(1 to 10).foreach(i => {
	      fileSink.recordEvent(QueryEvent("getRangeQuery" + i, true))
	      getRangeQuery()
	      fileSink.recordEvent(QueryEvent("getRangeQuery" + i, false))
	    })
		}
		fileSink.recordEvent(WarmupEvent(windowLengthInMinutes, false))
				
    /* Run some queries */
		val rangeSizes = List(10,100,1000)
		
		rangeSizes.foreach(currentRangeSize => {
			fileSink.recordEvent(ChangeRangeLengthEvent(currentRangeSize))
			
			(1 to 10).foreach(i => {
	      fileSink.recordEvent(QueryEvent("getRangeQuery" + i, true))
	      getRangeQuery()
	      fileSink.recordEvent(QueryEvent("getRangeQuery" + i, false))
	    })
		})

    //Flush trace messages to the file
    fileSink.flush()

    System.exit(0)
  }

	def withinWindow: Boolean = {
		val currentTime = System.nanoTime
		currentTime < beginningOfCurrentWindow + convertMinutesToNanoseconds(windowLengthInMinutes)
	}
	
	def convertMinutesToNanoseconds(minutes: Int): Long = {
		minutes.toLong * 60.toLong * 1000000000.toLong
	}
}
