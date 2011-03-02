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

object BinEventsIntoFiveMinuteWindows {
  val logger = Logger()
	var windowType = ""
	val windowLengthInMinutes = 5

  def main(args: Array[String]): Unit = {
		if(args.size < 2) {
	    println("Usage: BinEventsIntoFiveMinuteWindows <filename> <distinct or sliding>")
	    System.exit(1)
    }

		if (args(1) != "distinct" && args(1) != "sliding") {
			println("You must specify that the window type be either distinct or sliding.")
			System.exit(1)
		} else {
			windowType = args(1)
		}

		println("timestamp,thread,type,id,elapsedTime,minuteInterval,window")

		for (line <- Source.fromFile(args(0)).getLines) {
			if (!line.contains("timestamp")) {
				// print line & its window
				println(line + "," + getWindow(line))
			}
		}
	}

	def getWindow(line: String): Int = {
		val minute = getMinuteFromLine(line)
		
		if (windowType == "distinct") 
			return assignMinuteToDistinctWindow(minute)
		else
			return assignMinuteToSlidingWindow(minute)
	}
	
	def getMinuteFromLine(line: String): Int = {
		val lineEntries = line.split(",")
		lineEntries(5).toInt
	}
	
	def assignMinuteToSlidingWindow(minute: Int): Int = {
		0 // dummy implementation
	}
	
	def assignMinuteToDistinctWindow(minute: Int): Int = {
		(minute/windowLengthInMinutes).toInt
	}
}