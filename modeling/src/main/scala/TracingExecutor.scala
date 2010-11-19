package edu.berkeley.cs
package scads
package piql
package modeling

import comm._

abstract trait TracingExecutor extends QueryExecutor {
  val sink: FileTraceSink

  abstract override def apply(plan: QueryPlan)(implicit ctx: Context): QueryIterator = {
    new TracingIterator(super.apply(plan), plan.hashCode)
  }

  protected class TracingIterator(child: QueryIterator, planId: Int) extends QueryIterator {
    val name = "TracingIterator"

    /* Place a trace message on the queue of messages to be written to disk.  If space isn't available issue a warning */
    protected def recordTrace(operation: String): Boolean = {
      sink.recordEvent(IteratorEvent(
        child.name,
        planId,
        operation))
    }

    def open = {
      recordTrace("openStart")
      child.open
      recordTrace("openEnd")
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
