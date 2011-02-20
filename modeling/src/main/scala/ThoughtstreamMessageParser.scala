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

abstract class ThoughtstreamMessageParser extends SCADSMessageParser {
  var numSubscriptions = 0
  var numPerPage = 0
  
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
	
	override def processIteratorEvent(event: ExecutionTrace) = {
		val ExecutionTrace(timestamp, threadId, IteratorEvent(iteratorName, plan, op, start)) = event
		if (doneWithWarmup) {
			if (start)
				eventMap += (iteratorName -> timestamp)
			else {
				val eventBeginning = eventMap(iteratorName)
				val eventElapsedTime = timestamp - eventBeginning
				println(List(eventBeginning, threadId, "iterator", iteratorName + ":" + plan + ":" + op, eventElapsedTime, numSubscriptions, numPerPage).mkString(","))
			}
		}
	}

	override def processMessageEvent(event: ExecutionTrace) = {
		val ExecutionTrace(timestamp, threadId, MessageEvent(msg)) = event
		if (doneWithWarmup) {
			val messageName = msg.body.getSchema.getName
			if (messageName.contains("Request")) {
				eventMap += (messageNameWithTransitInfo(msg) -> timestamp)
			} else if (messageName.contains("Response")) {
				val eventBeginning = eventMap(messageNameWithTransitInfo(msg))
				val eventElapsedTime = timestamp - eventBeginning
				println(List(eventBeginning, threadId, "message", messageNameWithTransitInfo(msg), eventElapsedTime, numSubscriptions, numPerPage).mkString(","))
			}
		}
	}
}