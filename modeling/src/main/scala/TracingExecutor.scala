package edu.berkeley.cs
package scads
package piql

import avro.runtime._
import avro.marker._

import java.io.File
import java.util.concurrent.{ArrayBlockingQueue, TimeUnit}

sealed trait TracingMessage extends AvroUnion
case class QueryExecutionTrace(var timestamp: Long, var threadName: String, var iteratorName: String, var planId: Int, var operation: String) extends AvroRecord with TracingMessage
case class QueryStart(var name: String) extends TracingMessage
case class QueryEnd(var name: String)

abstract trait TracingExecutor extends QueryExecutor {
  val traceFile: File
  lazy protected val outputFile = AvroOutFile[TracingMessage](traceFile, CodecFactory.deflateCodec(5))

  /* A list of messages that will be written to disk async by a seperate thread */
  protected val pendingTraceMessages = new ArrayBlockingQueue[TracingMessage](1024)
  protected val ioThread = new Thread("QueryTraceWriter") {
    override def run(): Unit = {
      while(true) {
        outputFile.append(pendingTraceMessages.take)
      }
    }
  }
  ioThread.start

  def recordMessage(msg: TracingMessage): Unit = {
    val success = pendingTraceMessages.offer(msg, 0, TimeUnit.MILLISECONDS)
      if(!success)
        logger.warning("Failed to record trace message: %s", msg)
    success
  }

  def flush(): Unit = {
    while(pendingTraceMessages.size > 0) {
      logger.info("Waiting for %d pendingTraceMessages to be written.", pendingTraceMessages.size)
      Thread.sleep(100)
    }
    outputFile.flush
  }

  abstract override def apply(plan: QueryPlan)(implicit ctx: Context): QueryIterator = {
    new TracingIterator(super.apply(plan), plan.hashCode)
  }

  protected class TracingIterator(child: QueryIterator, planId: Int) extends QueryIterator {
    val name = "TracingIterator"

    /* Place a trace message on the queue of messages to be written to disk.  If space isn't available issue a warning */
    protected def recordTrace(operation: String): Boolean = {
      recordMessage(QueryExecutionTrace(
        System.nanoTime,
        Thread.currentThread.getName,
        child.name,
        planId,
        operation))
    }

    def open = {
      recordTrace("open")
      child.open
    }

    def close = {
      recordTrace("close")
      child.close
    }

    def hasNext = {
      recordTrace("hasNext")
      val childHasNext = child.hasNext
      logger.debug("%s hasNext: %b", child.name ,childHasNext)
      childHasNext
    }

    def next = {
      recordTrace("next")
      val nextValue = child.next
      logger.ifDebug {child.name + " next: " + nextValue.toList}
      nextValue
    }
  }
}
