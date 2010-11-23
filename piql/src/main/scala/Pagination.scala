package edu.berkeley.cs.scads.piql

import java.{ util => ju }

/**
 * A PageResult is returned for a paginated query.
 *
 * Note: Assumes iterator has not been open yet
 */
class PageResult(private val iterator: QueryIterator, val elemsPerPage: Int) extends Iterator[QueryResult] {
  assert(elemsPerPage > 0)

  var limitReached = false
  var curBuf: QueryResult = null

  def open: Unit = {
    iterator.open
  }

  def hasAnotherPage: Boolean = hasNext

  def nextPage: QueryResult = next

  def hasNext = {
    if (curBuf ne null) true
    else if (limitReached) false
    else {
      val builder = Seq.newBuilder[Tuple]
      var cnt = 0
      while (cnt < elemsPerPage && iterator.hasNext) {
        builder += iterator.next
        cnt += 1
      }
      if (cnt == 0) {
        limitReached = true
        //iterator.close
        false
      } else {
        curBuf = builder.result
        true
      }
    }
  }

  def next = {
    if (!hasNext) // pages in the next page if one exists
      throw new ju.NoSuchElementException("No results left")
    val res = curBuf
    curBuf = null
    res
  }
}
