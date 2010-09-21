package edu.berkeley.cs.scads.piql

import net.lag.logging.Logger

class DebugIterator(child: QueryIterator, logger: Logger) extends QueryIterator {
  def open = {
    logger.info("Open")
    child.open
  }
  def close = {
    logger.info("Close")
    child.close
  }
  def hasNext = {
    val childHasNext = child.hasNext
    logger.info("HasNext: %b", childHasNext)
    childHasNext
  }
  def next = {
    val nextValue = child.next
    logger.info("tuple: %s", nextValue.toList)
    nextValue
  }
}
