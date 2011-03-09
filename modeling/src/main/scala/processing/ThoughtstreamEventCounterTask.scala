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

import com.amazonaws.services.sns._
import com.amazonaws.auth._
import com.amazonaws.services.sns.model._

case class ThoughtstreamEventCounterTask(
  var clusterAddress: String,
  var baseUrl: String,
  var numTraceFiles: Int
) extends AvroTask with AvroRecord with SCADSMessageParser {
  var queryStartCounter = 0
  var queryEndCounter = 0

  var eventCountSummary:List[String] = Nil

  def run(): Unit = {
    println("Starting parsing...")
    
    val clusterRoot = ZooKeeperNode(clusterAddress)
    val coordination = clusterRoot.getOrCreate("coordination")
    val clientId = coordination.registerAndAwait("eventCounterStart", numTraceFiles)

    val traceFileUrl = baseUrl + clientId
    val inFile = AvroHttpFile[ExecutionTrace](traceFileUrl)

    inFile.foreach {
			case event @ ExecutionTrace(_, _, WarmupEvent(_, start)) => processWarmupEvent(event)
	    case event @ ExecutionTrace(timestamp, threadId, QueryEvent(queryName, start)) => processQueryEvent(event)
			case event @ ExecutionTrace(timestamp, threadId, ChangeNamedCardinalityEvent(name, numDataItems)) => processChangeNamedCardinalityEvent(event)
			case _ =>
    }

    // print info for last (#subscriptions, #perPage) pair
    println("    start=" + queryStartCounter + ", end=" + queryEndCounter)
    eventCountSummary = ("    start=" + queryStartCounter + ", end=" + queryEndCounter) :: eventCountSummary
        
    // Publish to SNSClient
    ExperimentNotification.completions.publish("experiment completed at " + System.currentTimeMillis, eventCountSummary.reverse.mkString("\n"))
    
    println("Finished with trace collection.")
  }
  
  def processChangeNamedCardinalityEvent(event: ExecutionTrace) = {
		val ExecutionTrace(timestamp, threadId, ChangeNamedCardinalityEvent(name, numDataItems)) = event
		val prefix = name match {
		  case "numSubscriptions" => ""
		  case "numPerPage" => "  "
		}
		if (queryStartCounter > 0 && queryEndCounter > 0) {
      println("    start=" + queryStartCounter + ", end=" + queryEndCounter)
		  val line = "    start=" + queryStartCounter + ", end=" + queryEndCounter
      eventCountSummary = line :: eventCountSummary
		}
    println(prefix + name + ": " + numDataItems)
    eventCountSummary = (prefix + name + ": " + numDataItems) :: eventCountSummary
    queryStartCounter = 0
    queryEndCounter = 0
	}
	
	override def processQueryEvent(event: ExecutionTrace) = {
		val ExecutionTrace(timestamp, threadId, QueryEvent(queryName, start)) = event
		if (doneWithWarmup) {
			if (start)
			  queryStartCounter += 1
			else
			  queryEndCounter += 1
		}
	}
  
}
