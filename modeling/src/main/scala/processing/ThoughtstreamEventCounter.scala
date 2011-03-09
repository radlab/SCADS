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

object ThoughtstreamEventCounter extends ThoughtstreamMessageParser {
  var queryStartCounter = 0
  var queryEndCounter = 0

  def main(args: Array[String]): Unit = {
    if(args.size != 1) {
      println("Usage: EventCounter <url>")
      System.exit(1)
    }

    val traceFileUrl = args(0)
    val inFile = AvroHttpFile[ExecutionTrace](traceFileUrl)

    inFile.foreach {
			case event @ ExecutionTrace(_, _, WarmupEvent(_, start)) => processWarmupEvent(event)
	    case event @ ExecutionTrace(timestamp, threadId, QueryEvent(queryName, start)) => processQueryEvent(event)
			case event @ ExecutionTrace(timestamp, threadId, ChangeNamedCardinalityEvent(name, numDataItems)) => processChangeNamedCardinalityEvent(event)
			case _ =>
    }

    // print info for last (#subscriptions, #perPage) pair
    println("    start=" + queryStartCounter + ", end=" + queryEndCounter)
    System.exit(0)
  }

  override def processChangeNamedCardinalityEvent(event: ExecutionTrace) = {
		val ExecutionTrace(timestamp, threadId, ChangeNamedCardinalityEvent(name, numDataItems)) = event
		val prefix = name match {
		  case "numSubscriptions" => ""
		  case "numPerPage" => "  "
		}
		if (queryStartCounter > 0 && queryEndCounter > 0) 
      println("    start=" + queryStartCounter + ", end=" + queryEndCounter)
    println(prefix + name + ": " + numDataItems)
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
