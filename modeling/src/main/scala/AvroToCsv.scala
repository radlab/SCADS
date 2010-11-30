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

object AvroToCsv {
  val logger = Logger()

  def main(args: Array[String]): Unit = {
    if(args.size != 1) {
      println("Usage: AvroToCsv <filename>")
      System.exit(1)
    }

    val traceFile = new File(args(0))
    val inFile = AvroInFile[ExecutionTrace](traceFile)
    println("timestamp, thread, operationLevel, operationType, iteratorFunctionCall, start")
    inFile.map {
      case ExecutionTrace(timestamp, threadId, QueryEvent(queryName, start)) => List(timestamp, threadId, "query", queryName, 0, start).mkString(",")
      case ExecutionTrace(timestamp, threadId, IteratorEvent(iteratorName, plan, op, start)) => List(timestamp, threadId, "iterator", iteratorName, op, start).mkString(",")
      case ExecutionTrace(timestamp, threadId, MessageEvent(msg)) => List(timestamp, threadId, "message", msg.body.getSchema.getName, 0, 0).mkString(",")
    }.foreach(println)

    System.exit(0)
  }
}
