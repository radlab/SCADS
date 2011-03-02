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


object ThoughtstreamMessageParserBasic extends ThoughtstreamMessageParser {
  var numQueriesToOutput = 0
  var queryCounter = 0
  var desiredNumSubscriptions = 0
  var desiredNumPerPage = 0
  
  def main(args: Array[String]): Unit = {
  	if(args.size != 4) {
        println("Usage: ThoughtstreamMessageParserBasic <url> <numQueries> <desiredNumSubscriptions> <desiredNumPerPage>")
        System.exit(1)
    }
    
    setupFields(args)
    
    val traceFileUrl = args(0)
    val inFile = AvroHttpFile[ExecutionTrace](traceFileUrl)

  	println("timestamp,thread,type,id,elapsedTime,numSubscriptions,numPerPage")

    inFile.foreach {
  			case event @ ExecutionTrace(_, _, WarmupEvent(_, start)) => processWarmupEvent(event)
  	    case event @ ExecutionTrace(timestamp, threadId, QueryEvent(queryName, start)) => processQueryEvent(event)
	      case event @ ExecutionTrace(timestamp, threadId, IteratorEvent(iteratorName, plan, op, start)) => processIteratorEvent(event)
	      case event @ ExecutionTrace(timestamp, threadId, MessageEvent(msg)) => processMessageEvent(event)
  			case event @ ExecutionTrace(timestamp, threadId, ChangeNamedCardinalityEvent(name, numDataItems)) => processChangeNamedCardinalityEvent(event)
  			case _ =>
  	}
  }
  
  override def processQueryEvent(event:ExecutionTrace) = {
		val ExecutionTrace(timestamp, threadId, QueryEvent(queryName, start)) = event
    if (shouldInclude(event)) {
      super.processQueryEvent(event)
      if (!start)
        queryCounter += 1
    }
  }
  
  override def processIteratorEvent(event:ExecutionTrace) = {
    if (shouldInclude(event))
      super.processIteratorEvent(event)
  }
  
  override def processMessageEvent(event:ExecutionTrace) = {
    if (shouldInclude(event))
      super.processMessageEvent(event)
  }
  
  def shouldInclude(event:ExecutionTrace) = {
    (doneWithWarmup && 
      queryCounter < numQueriesToOutput && 
      numSubscriptions == desiredNumSubscriptions && 
      numPerPage == desiredNumPerPage
    )
  }
  
  def setupFields(args:Array[String]) = {
    numQueriesToOutput = args(1).toInt
    desiredNumSubscriptions = args(2).toInt
    desiredNumPerPage = args(3).toInt
  }
}

