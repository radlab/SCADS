package edu.berkeley.cs
package scads
package piql
package modeling

import avro.runtime._
import avro.marker._
import comm._

import java.io.File
import java.util.concurrent.{ArrayBlockingQueue, TimeUnit}

import net.lag.logging.Logger
import org.apache.avro.file.CodecFactory


class FileTraceSink(val traceFile: File) {
  protected val logger = Logger()
  protected val outputFile = AvroOutFile[ExecutionTrace](traceFile, CodecFactory.deflateCodec(5))

  /* A list of messages that will be written to disk async by a seperate thread */
  protected val pendingTraceMessages = new ArrayBlockingQueue[ExecutionTrace](1024)
  protected val ioThread = new Thread("QueryTraceWriter") {
    override def run(): Unit = {
      while(true) {
        outputFile.append(pendingTraceMessages.take)
      }
    }
  }
  ioThread.start

  def recordEvent(event: TraceEvent): Boolean = {
    val trace = ExecutionTrace(System.nanoTime, Thread.currentThread.getName, event)
    val success = pendingTraceMessages.offer(trace, 0, TimeUnit.MILLISECONDS)
      if(!success)
        logger.warning("Failed to record trace event: %s", event)
    success
  }

  def flush(): Unit = {
    while(pendingTraceMessages.size > 0) {
      logger.info("Waiting for %d pendingTraceMessages to be written.", pendingTraceMessages.size)
      Thread.sleep(100)
    }
    outputFile.flush
  }
}
