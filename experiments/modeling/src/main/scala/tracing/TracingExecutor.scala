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
    protected def recordTrace(operation: String, start: Boolean): Boolean = {
      sink.recordEvent(IteratorEvent(
        child.name,
        planId,
        operation,
	start))
    }

    def open = {
      recordTrace("open", true)
      child.open
      recordTrace("open", false)
    }

    def close = {
      recordTrace("close", true)
      child.close
      recordTrace("close", false)
    }

    def hasNext = {
      recordTrace("hasNext", true)
      val childHasNext = child.hasNext
      recordTrace("hasNext", false)
      childHasNext
    }

    def next = {
      recordTrace("next", true)
      val nextValue = child.next
      recordTrace("next", false)
      nextValue
    }
  }
}
