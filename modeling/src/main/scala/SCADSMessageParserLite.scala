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

/*
 *	Lite SCADS message parser outputs info just for queries (wrt boundary events).
 */
object SCADSMessageParserLite extends SCADSMessageParser {
  def main(args: Array[String]): Unit = {
		if(args.size != 1) {
	      println("Usage: SCADSMessageParserLite <filename>")
	      System.exit(1)
    }

	  val traceFile = new File(args(0))
	  val inFile = AvroInFile[ExecutionTrace](traceFile)

		println(headerRow)

	  inFile.foreach {
				case event @ ExecutionTrace(_, _, WarmupEvent(_, start)) => processWarmupEvent(event)
		    case event @ ExecutionTrace(timestamp, threadId, QueryEvent(queryName, start)) => processQueryEvent(event)
				case event @ ExecutionTrace(timestamp, threadId, ChangeCardinalityEvent(numDataItems)) => processChangeCardinalityEvent(event)
				case _ =>
		}
  }


	
	
}