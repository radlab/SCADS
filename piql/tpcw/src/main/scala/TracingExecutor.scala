package edu.berkeley.cs
package scads
package piql
package tpcw
package scale

import piql.exec._
import piql.plans._

abstract trait TracingExecutor extends QueryExecutor {
  val sink: FileTraceSink

  abstract override def apply(plan: QueryPlan)(implicit ctx: Context): QueryIterator = {
    if (storage.shouldSampleTrace) {
      new TracingIterator(super.apply(plan))
    } else {
      super.apply(plan)
    }
  }

  protected class TracingIterator(child: QueryIterator) extends QueryIterator {
    val name = "TracingIterator"

    def recordSpan[A,B](opname: String)(block: => B): B = {
      var start = System.nanoTime
      try {
        block
      } finally {
        sink.recordEvent(IteratorSpan(
          child.name,
          opname,
          System.nanoTime - start), start)
      }
    }

    def open = recordSpan("open")(child.open)

    def close = recordSpan("close")(child.close)

    def hasNext = recordSpan("hasNext")(child.hasNext)

    def next = recordSpan("next")(child.next)
  }
}
