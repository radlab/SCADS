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

case class R1(var f1: Int) extends AvroPair
case class R2(var f1: Int, var f2: Int) extends AvroPair

object TracingExample {

  val logger = Logger()
  def main(args: Array[String]): Unit = {
    /* Connect to scads cluster */
    val cluster = args.size match {
      case 0 => TestScalaEngine.newScadsCluster()
      case 1 => new ScadsCluster(ZooKeeperNode(args(0)))
      case _ => {
	println("Usage: [cluster address]")
	System.exit(1)
	return
      }
    }

    /* get namespaces */
    val r1 = cluster.getNamespace[R1]("r1")
    val r2 = cluster.getNamespace[R2]("r2")

    /* create executor that records trace to fileSink */
    val fileSink = new FileTraceSink(new File("piqltrace.avro"))
    implicit val executor = new ParallelExecutor with TracingExecutor {
      val sink = fileSink
    }

    /* Register a listener that will record all messages sent/recv to fileSink */
    val messageTracer = new MessagePassingTracer(fileSink)
    MessageHandler.registerListener(messageTracer)

    /* Bulk load some test data into the namespaces */
    r1 ++= (1 to 10).view.map(i => R1(i))
    r2 ++= (1 to 10).view.flatMap(i => (1 to 10).map(j => R2(i,j)))

    /**
     * Write queries against relations and create optimized function using .toPiql
     * toPiql uses implicit executor defined above to run queries
     */
    val getQuery = r1.where("f1".a === 0).toPiql
    val getRangeQuery = r2.where("f1".a === 0)
			  .limit(10).toPiql

  
    /* Run some queries */
    (1 to 10).foreach(i => {
      fileSink.recordEvent(QueryEvent("getQuery" + i, "start"))
      getQuery(Nil)
      fileSink.recordEvent(QueryEvent("getQuery" + i, "end"))

      fileSink.recordEvent(QueryEvent("getRangeQuery" + i, "start"))
      getRangeQuery(Nil)
      fileSink.recordEvent(QueryEvent("getRangeQuery" + i, "end"))
    })
    
    //Flush trace messages to the file
    fileSink.flush()

    /* Read back in trace file */
    val inFile = AvroInFile[ExecutionTrace](fileSink.traceFile)
    inFile.foreach(println)
    System.exit(0)}
    
}
