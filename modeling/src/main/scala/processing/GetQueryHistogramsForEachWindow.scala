package edu.berkeley.cs
package scads
package piql
package modeling

import comm._
import storage._
import piql._
import perf._
import avro.marker._
import avro.runtime._

import net.lag.logging.Logger

import java.io.File

//import java.io.File
import scala.io.Source
import scala.collection.mutable._

object GetQueryHistogramsForEachWindow {
  val logger = Logger()

	var getQueryHistogram:Histogram = null
	var getRangeQueryHistogram:Histogram = null
	var joinQueryHistogram:Histogram = null

	setUpHistograms

	var windowStartTime = 0.toLong
	var windowInitialized = false
	var currentWindowNum = 0
	
	val windowLengthInMinutes = 5

	def setUpHistograms = {
		getQueryHistogram = Histogram(1,1000)
		getRangeQueryHistogram = Histogram(1,1000)
		joinQueryHistogram = Histogram(1,1000)
	}

  def main(args: Array[String]): Unit = {
		if(args.size != 1) {
	      println("Usage: GetElapsedTimeForQueryEvents <filename>")
	      System.exit(1)
	    }
	
	  val traceFile = new File(args(0))
	  val inFile = AvroInFile[ExecutionTrace](traceFile)
	
		val queryMap = HashMap[String, Long]()
	
	  inFile.foreach {
	    case ExecutionTrace(timestamp, threadId, QueryEvent(queryName, start)) => {
				if (!windowInitialized) {
					windowStartTime = timestamp
					windowInitialized = true
				}
		
				// I can work with all these variables:  timestamp, threadId, QueryEvent(queryName, start)
				if (start) {
					queryMap += (queryName -> timestamp)
				} else {
					if (!withinCurrentWindow(timestamp)) {
						
						// print out histograms
						//println(currentWindowNum)
						println("getQuery" + currentWindowNum.toString + "," + currentWindowNum.toString + "," + getQueryHistogram.toString)
						println("getRangeQuery" + currentWindowNum.toString + "," + currentWindowNum.toString + "," + getRangeQueryHistogram.toString)
						println("joinQuery" + currentWindowNum.toString + "," + currentWindowNum.toString + "," + joinQueryHistogram.toString)
						//println

						// reset histograms
						setUpHistograms
						
						// reset window start time
						resetWindowStartTime
						currentWindowNum += 1
					}
						
					val eventBeginning = queryMap(queryName)
					val eventElapsedTime = timestamp - eventBeginning

					if (queryName.contains("getQuery")) {
						//println(queryName)
						getQueryHistogram.add(convertNanosecondsToMilliseconds(eventElapsedTime))
					} else if (queryName.contains("getRangeQuery")) {
						//println(queryName)
						getRangeQueryHistogram.add(convertNanosecondsToMilliseconds(eventElapsedTime))
					} else if (queryName.contains("joinQuery")) {
						joinQueryHistogram.add(convertNanosecondsToMilliseconds(eventElapsedTime))
					}
					
				}
			}
		case _ => 
		}
	}
	
	def convertBooleanToStartOrEnd(startBoolean: Boolean): String = {
		if (startBoolean)
	  	return "start"
		else
	  	return "end"
  }

	def withinCurrentWindow(timestamp: Long): Boolean = {
		//println("checking " + timestamp.toString + " < " + windowStartTime.toString + " + " + convertMinutesToNanoseconds(windowLengthInMinutes).toString)
		//println("answer is " + (timestamp < windowStartTime + convertMinutesToNanoseconds(windowLengthInMinutes)).toString)
		timestamp < windowStartTime + convertMinutesToNanoseconds(windowLengthInMinutes)
	}
  
	def convertMinutesToNanoseconds(minutes: Int): Long = {
		minutes.toLong * 60.toLong * 1000000000.toLong
	}
	
	def resetWindowStartTime = {
		windowStartTime += convertMinutesToNanoseconds(windowLengthInMinutes)
	}
	
	def convertNanosecondsToMilliseconds(nanoseconds: Long): Long = {
		nanoseconds/(1000000.toLong)
	}
}