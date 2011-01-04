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

import scala.io.Source
import scala.collection.mutable._

object BinEventsIntoOneMinuteIntervals {
  val logger = Logger()
	var baseTimestamp = 0.toLong
	var baseTimestampSet = false

  def main(args: Array[String]): Unit = {
		if(args.size != 1) {
	      println("Usage: BinEventsIntoOneMinuteIntervals <filename>")
	      System.exit(1)
    }

		println("timestamp,thread,type,id,elapsedTime,minuteInterval")

		for (line <- Source.fromFile(args(0)).getLines) {
			// print line & its minute interval
			println(line + "," + getMinuteInterval(line))
		}
	}
	
	//def getMinuteInterval(line: String): Int = {
	def getMinuteInterval(line: String) = {	
		if (!baseTimestampSet) {
			setBaseTimestamp(line)
		}

		val currentNormalizedTimestamp = getTimestampFromLine(line) - baseTimestamp
		convertNormalizedTimestampToMinute(currentNormalizedTimestamp)
	}
	
	def setBaseTimestamp(line: String) = {
	  baseTimestamp = getTimestampFromLine(line)
	}

	def getTimestampFromLine(line: String): Long = {
		val lineEntries = line.split(",")
		lineEntries(0).toLong
	}
	
	def convertNormalizedTimestampToMinute(timestamp: Long): Int = {
		// since normalized timestamp is in ns, need to convert to minutes
		// first, convert to seconds
		val timeInSeconds = timestamp/(1000000000.0)
		
		// then, convert to minutes
		val timeInMinutes = timeInSeconds/60
		
		timeInMinutes.toInt
	}
}
