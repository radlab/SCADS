package edu.berkeley.cs
package radlab
package demo

import scads.piql._

object DashboardReportingExecutor {

}

/**
 * Records the elapsed time between open and close and reports it to the dashboard.
 */
class DashboardReportingExecutor extends QueryExecutor {
  val delegate = new ParallelExecutor

  def apply(plan: QueryPlan)(implicit ctx: Context): QueryIterator =
    new QueryIterator {
      val name = "DashboardReportingExecutor"
      private val childIterator = delegate(plan)
      private var startTime = 0L

      def open: Unit = {
	startTime = System.nanoTime
	childIterator.open
      }

      def close: Unit = {
	childIterator.close
	val endTime = System.nanoTime

	logger.info("Query Executed in %d nanoseconds.", endTime - startTime)
      }

      def hasNext = childIterator.hasNext
      def next = childIterator.next
    }
}
