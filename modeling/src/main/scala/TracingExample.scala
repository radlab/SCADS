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
    val cluster = TestScalaEngine.getTestCluster
    val executor = new ParallelExecutor with TracingExecutor {
      val traceFile = new File("piqltrace.avro")
    }
    val client = new ScadrClient(cluster, executor, maxSubscriptions=10)

    /* Sample data */
    client.users.put(UserKey("marmbrus"), UserValue("Berkeley"))
    client.users.put(UserKey("kcurtis"), UserValue("Berkeley"))
    client.subscriptions.put(SubscriptionKey("marmbrus", "kcurtis"), SubscriptionValue(approved=true))
    client.subscriptions.put(SubscriptionKey("kcurtis", "marmbrus"), SubscriptionValue(approved=true))
    client.thoughts.put(ThoughtKey("marmbrus", 1), ThoughtValue("Soon there will be new syntax that joins the key and the value into one class"))
  
    /* Run some queries */
    logger.info("Running thoughtstream query 10 times")
    (1 to 10).foreach(i => client.thoughtstream("kcurtis", 10))

    //Flush trace messages to the file
    executor.flush()

    val inFile = AvroInFile[QueryExecutionTrace](executor.traceFile)
    inFile.foreach(println)
    System.exit(0)
  }
}
