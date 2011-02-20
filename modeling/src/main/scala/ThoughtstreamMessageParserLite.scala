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


class ThoughtstreamMessageParserLite extends ThoughtstreamMessageParser {
  def main(args: Array[String]): Unit = {
  	if(args.size != 1) {
        println("Usage: ThoughtstreamMessageParserLite <url>")
        System.exit(1)
    }

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
}

