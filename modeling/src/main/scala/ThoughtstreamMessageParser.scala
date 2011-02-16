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
import scala.io.Source
import scala.collection.mutable._

import org.apache.commons.httpclient._
import org.apache.commons.httpclient.methods._

import org.apache.avro.specific.{SpecificDatumReader, SpecificDatumWriter}
import org.apache.avro.file.DataFileStream


/*
 *	Handles info for thoughtstream queries.  Necessary because of new event type, ""
 */
 
// case class ChangeNamedCardinalityEvent(var nameOfCardinality: String, var numDataItems: Int) extends AvroRecord with TraceEvent

object ThoughtstreamMessageParser extends SCADSMessageParser {
  var numSubscriptions = 0
  var numPerPage = 0
  
  def main(args: Array[String]): Unit = {
		if(args.size != 1) {
	      println("Usage: ThoughtstreamMessageParser <url>")
	      System.exit(1)
    }

	  //val traceFile = new File(args(0))
	  //val inFile = AvroInFile[ExecutionTrace](traceFile)
	  val traceFileUrl = args(0)
	  val inFile = AvroHttpFile[ExecutionTrace](traceFileUrl)

		println("timestamp,thread,type,id,elapsedTime,numSubscriptions,numPerPage")

	  inFile.foreach {
				case event @ ExecutionTrace(_, _, WarmupEvent(_, start)) => processWarmupEvent(event)
		    case event @ ExecutionTrace(timestamp, threadId, QueryEvent(queryName, start)) => processQueryEvent(event)
				case event @ ExecutionTrace(timestamp, threadId, ChangeNamedCardinalityEvent(name, numDataItems)) => processChangeNamedCardinalityEvent(event)
				case _ =>
		}
  }
  
  def processChangeNamedCardinalityEvent(event: ExecutionTrace) = {
		val ExecutionTrace(timestamp, threadId, ChangeNamedCardinalityEvent(name, numDataItems)) = event
		name match {
		  case "numSubscriptions" => numSubscriptions = numDataItems
		  case "numPerPage" => numPerPage = numDataItems
		  case _ => println("invalid cardinality event name")
		}
	}
	
	override def processQueryEvent(event: ExecutionTrace) = {
		val ExecutionTrace(timestamp, threadId, QueryEvent(queryName, start)) = event
		if (doneWithWarmup) {
			if (start)
				eventMap += (queryName -> timestamp)
			else {
				val eventBeginning = eventMap(queryName)
				val eventElapsedTime = timestamp - eventBeginning
				println(List(eventBeginning, threadId, "query", queryName, eventElapsedTime, numSubscriptions, numPerPage).mkString(","))
			}
		}
	}
}