package edu.berkeley.cs.scads.piql
package debug

import exec._
import plans._

import net.lag.logging.Logger

/**
 * Wrapper to add functions to piql tuples.
 */
class RichTuple(t: Tuple) {
  def get(aliasPrefix: String, fieldName: String): Any = {
    val rec = t.find(r => (r.getSchema.getName startsWith aliasPrefix) && (r.getSchema.getField(fieldName) != null)).getOrElse(
      throw new RuntimeException("Can't find field: " + aliasPrefix + "." + fieldName)
    )
    rec.get(rec.getSchema.getField(fieldName).pos)
  }
}


/**
 * Mix-in executor that interposes debugging statements in between all iterator operations.
 */
trait DebugExecutor extends QueryExecutor {

  abstract override def apply(plan: QueryPlan)(implicit ctx: Context): QueryIterator = {
    new DebugIterator(super.apply(plan))
  }

  class DebugIterator(child: QueryIterator) extends QueryIterator {
    val name = "DebugPrintIterator"

    def open = {
      logger.debug("Opening %s", child.name)
      child.open
    }

    def close = {
      logger.debug("Closing %s", child.name)
      child.close
    }

    def hasNext = {
      val childHasNext = child.hasNext
      logger.debug("%s hasNext: %b", child.name ,childHasNext)
      childHasNext
    }

    def next = {
      val nextValue = child.next
      logger.ifDebug {child.name + " next: " + nextValue.toList}
      nextValue
    }
  }
}
