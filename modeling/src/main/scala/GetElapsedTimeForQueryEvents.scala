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

object GetElapsedTimeForQueryEvents {
  val logger = Logger()
	val eventMap = HashMap[String, Long]()
	var doneWithWarmup = false

  def main(args: Array[String]): Unit = {
		if(args.size != 1) {
	      println("Usage: GetElapsedTimeForQueryEvents <filename>")
	      System.exit(1)
    }

	  val traceFile = new File(args(0))
	  val inFile = AvroInFile[ExecutionTrace](traceFile)

		println("timestamp,thread,type,id,elapsedTime")

	  inFile.foreach {
				case event @ ExecutionTrace(_, _, WarmupEvent(_, start)) => processWarmupEvent(event)
		    case event @ ExecutionTrace(timestamp, threadId, QueryEvent(queryName, start)) => processQueryEvent(event)
	      case event @ ExecutionTrace(timestamp, threadId, IteratorEvent(iteratorName, plan, op, start)) => processIteratorEvent(event)
	      case event @ ExecutionTrace(timestamp, threadId, MessageEvent(msg)) => processMessageEvent(event)
				case _ =>
		}
  }

	def processWarmupEvent(event: ExecutionTrace) = {
		val ExecutionTrace(_, _, WarmupEvent(_, start)) = event
		if (!start) {
			doneWithWarmup = true
		}
	}

	def processQueryEvent(event: ExecutionTrace) = {
		val ExecutionTrace(timestamp, threadId, QueryEvent(queryName, start)) = event
		if (doneWithWarmup) {
			if (start)
				eventMap += (queryName -> timestamp)
			else {
				val eventBeginning = eventMap(queryName)
				val eventElapsedTime = timestamp - eventBeginning
				println(List(eventBeginning, threadId, "query", queryName, eventElapsedTime).mkString(","))
			}
		}
	}
	
	def processIteratorEvent(event: ExecutionTrace) = {
		val ExecutionTrace(timestamp, threadId, IteratorEvent(iteratorName, plan, op, start)) = event
		if (doneWithWarmup) {
			if (start)
				eventMap += (iteratorName -> timestamp)
			else {
				val eventBeginning = eventMap(iteratorName)
				val eventElapsedTime = timestamp - eventBeginning
				println(List(eventBeginning, threadId, "iterator", iteratorName + ":" + plan + ":" + op, eventElapsedTime).mkString(","))
			}
		}
	}

	def processMessageEvent(event: ExecutionTrace) = {
		val ExecutionTrace(timestamp, threadId, MessageEvent(msg)) = event
		//msg.body.getSchema.getName, "Src:" + msg.src.get, "Dest:" + msg.dest
		if (doneWithWarmup) {
			val messageName = msg.body.getSchema.getName
			if (messageName.contains("Request")) {
				eventMap += (messageNameWithTransitInfo(msg) -> timestamp)
			} else if (messageName.contains("Response")) {
				val eventBeginning = eventMap(messageNameWithTransitInfo(msg))
				val eventElapsedTime = timestamp - eventBeginning
				println(List(eventBeginning, threadId, "message", messageNameWithTransitInfo(msg), eventElapsedTime).mkString(","))
			}
		}
	}

	def messageNameWithTransitInfo(msg:Message):String = {
		// get message name
		val messageStatus = getMessageStatus(msg)
		val messageType = getMessageType(msg.body.getSchema.getName, messageStatus)
		
		// get message transit info
		val messageTransitInfo = getMessageTransitInfo(msg.src.get.toString, msg.dest.toString, messageStatus)
		
		// construct name based on whether it's a request or response
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
	
	def getMessageStatus(msg:Message):String = {
		if (msg.body.getSchema.getName.contains("Request"))
			"Request"
		else if (msg.body.getSchema.getName.contains("Response"))
			"Response"
		else
			"Invalid status"
	}
	
	
}