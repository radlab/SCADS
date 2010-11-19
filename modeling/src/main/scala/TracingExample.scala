package edu.berkeley.cs
package scads
package piql
package modeling

import comm._
import storage._
import piql._
import avro.runtime._

import net.lag.logging.Logger

import java.io.File

object TracingExample {

  val logger = Logger()
  def main(args: Array[String]): Unit = {
    if(args.size != 1) {
      println("Usage: <cluster address>")
      System.exit(1)
    }

    val cluster = new ScadsCluster(ZooKeeperNode(args(0)))
    val fileSink = new FileTraceSink(new File("piqltrace.avro"))
    val executor = new ParallelExecutor with TracingExecutor {
      val sink = fileSink
    }

    val messageTracer = new MessagePassingTracer(fileSink)
    MessageHandler.registerListener(messageTracer)

    val client = new ScadrClient(cluster, executor, maxSubscriptions=10)

    /* Sample data */
    client.users.put(UserKey("marmbrus"), UserValue("Berkeley"))
    client.users.put(UserKey("kcurtis"), UserValue("Berkeley"))
    client.subscriptions.put(SubscriptionKey("marmbrus", "kcurtis"), SubscriptionValue(approved=true))
    client.subscriptions.put(SubscriptionKey("kcurtis", "marmbrus"), SubscriptionValue(approved=true))
    client.thoughts.put(ThoughtKey("marmbrus", 1), ThoughtValue("Soon there will be new syntax that joins the key and the value into one class"))
  
    /* Run some queries */
    fileSink.recordEvent(QueryEvent("thoughtstream", "start"))
    client.thoughtstream("kcurtis", 10)
    fileSink.recordEvent(QueryEvent("thoughtstream", "end"))

    
    //Flush trace messages to the file
    fileSink.flush()

    val inFile = AvroInFile[ExecutionTrace](fileSink.traceFile)
    inFile.foreach(println)
    System.exit(0)}
    
}
