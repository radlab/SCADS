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

//import java.io.File
import scala.io.Source
import scala.collection.mutable._

object GetElapsedTimeForQueryEvents {
  val logger = Logger()

  def main(args: Array[String]): Unit = {
		if(args.size != 1) {
	      println("Usage: GetElapsedTimeForQueryEvents <filename>")
	      System.exit(1)
	    }

		val eventMap = HashMap[String, String]()

		for (line <- Source.fromFile(args(0)).getLines) {
		  val lineEntries = line.split(",").toList

			if (lineEntries.exists(s => s == "query" || s == "iterator")) {
				if (lineEntries.exists(s => s == "start")) {
					eventMap += processQueryOrIteratorStartEvent(line)
				} else if (lineEntries.exists(s => s == "end")) {
					processQueryOrIteratorEndEvent(line, eventMap)
				}
			} else if (lineEntries.exists(s => s == "message")) {
				if (line.contains("Request")) {
					eventMap += processMessageStartEvent(line)
				} else if (line.contains("Response")) {
					processMessageEndEvent(line, eventMap)
				}
				// any other cases?
			}

		}
    
  }

	def processQueryOrIteratorStartEvent(event:String):Tuple2[String, String] = {
		(getEventId(event) -> getEventTimestamp(event))
	}
	
	def processQueryOrIteratorEndEvent(event:String, eventMap:Map[String, String]) = {
		val eventBeginning = eventMap(getEventId(event)).toLong
		val eventElapsedTime = getEventTimestamp(event).toLong - eventBeginning
		println(eventBeginning + "," + getEventThread(event) + "," + getEventType(event) + "," + getEventId(event) + "," + eventElapsedTime)
	}
	
	def getEventId(event:String):String = {
		event.split(",")(3)
	}
	
	def getEventTimestamp(event:String):String = {
		event.split(",")(0)
	}
	
	def getEventThread(event:String):String = {
		event.split(",")(1)
	}
	
	def getEventType(event:String):String = {
		event.split(",")(2)
	}
	
	def processMessageStartEvent(event:String):Tuple2[String, String] = {
		(getMessageEventId(event, "Request") -> getEventTimestamp(event))
	}
	
	def processMessageEndEvent(event:String, eventMap:Map[String, String]) = {
		//println(eventMap.keySet)

		val messageEventId = getMessageEventId(event, "Response")
		val eventBeginning = eventMap(messageEventId).toLong
		val eventElapsedTime = getEventTimestamp(event).toLong - eventBeginning
		println(eventBeginning + "," + getEventThread(event) + "," + getEventType(event) + "," + messageEventId + "," + eventElapsedTime)
	}
	
	def getMessageEventId(event:String, messageStatus:String):String = {
		val eventEntries = event.split(",")
		val messageType = getMessageType(eventEntries(3), messageStatus)
		val messageTransitInfo = getMessageTransitInfo(eventEntries(4), eventEntries(5), messageStatus)
		messageType + ":" + messageTransitInfo
	}
	
	def getMessageType(messageTypeAndStatus:String, messageStatus:String):String = {
		val statusIndex = messageTypeAndStatus.indexOf(messageStatus)
		messageTypeAndStatus.substring(0,statusIndex)
	}
	
	def getMessageTransitInfo(messageSrc:String, messageDest:String, messageStatus:String):String = {
		val srcNum = getSrcOrDestNum(messageSrc)
		val destNum = getSrcOrDestNum(messageDest)
		
		val messageTransitInfo = 
		if (messageStatus == "Request") {
			"Local=" + srcNum + ":Remote=" + destNum
		} else if (messageStatus == "Response") {
			"Local=" + destNum + ":Remote=" + srcNum
		}
		messageTransitInfo.toString
	}
	
	def getSrcOrDestNum(messageSrc:String):String = {
		val srcEntries = messageSrc.split(": ")
		val srcNum = srcEntries(1).split("}")(0)
		srcNum
	}
}