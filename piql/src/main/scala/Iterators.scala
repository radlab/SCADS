package edu.berkeley.cs.scads.piql

import net.lag.logging.Logger
import org.apache.avro.util.Utf8
import org.apache.avro.generic.{GenericData, IndexedRecord}

import edu.berkeley.cs.scads.comm.ScadsFuture

import java.{ util => ju }

case class Context(parameters: Array[Any], state: Option[List[Any]])

abstract class QueryIterator extends Iterator[Tuple] {
  val name: String
  def open: Unit
  def close: Unit
}

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

trait QueryExecutor {
  protected val logger = Logger("edu.berkeley.cs.scads.piql.QueryExecutor")

  def apply(plan: QueryPlan, args: Any*): QueryIterator = apply(plan)(Context(args.toArray, None))
  def apply(plan: QueryPlan)(implicit ctx: Context): QueryIterator

  protected def bindValue(value: Value, currentTuple: Tuple)(implicit ctx: Context): Any = value match {
    case FixedValue(v) => v
    case ParameterValue(o) => ctx.parameters(o)
    case AttributeValue(recPos, fieldPos) => currentTuple(recPos).get(fieldPos)
  }

  protected def bindKey(ns: Namespace, key: KeyGenerator, currentTuple: Tuple = null)(implicit ctx: Context): GenericData.Record = {
    val boundKey = ns.newKeyInstance
    key.map(bindValue(_, currentTuple)).zipWithIndex.foreach {
      case (value: Any, idx: Int) => boundKey.put(idx, value)
    }
    boundKey
  }

  protected def bindLimit(limit: Limit)(implicit ctx: Context): Int = limit match {
    case FixedLimit(l) => l
    case ParameterLimit(lim, max) => {
      val limitValue = ctx.parameters(lim).asInstanceOf[Int]
      if(limitValue > max)
        throw new RuntimeException("Limit out of range")
      limitValue
    }
  }

  protected def evalPredicate(predicate: Predicate, tuple: Tuple)(implicit ctx: Context): Boolean = predicate match {
    case Equality(v1, v2) => compareAny(bindValue(v1, tuple), bindValue(v2, tuple)) == 0
  }

  protected def compareTuples(left: Tuple, right: Tuple, attributes: Seq[AttributeValue])(implicit ctx: Context): Int = {
    attributes.foreach(a => {
      val leftValue = bindValue(a, left)
      val rightValue = bindValue(a, right)
      val comparison = compareAny(leftValue, rightValue)
      if(comparison != 0)
        return comparison
    })
    return 0
  }

  protected def compareAny(left: Any, right: Any): Int = (left, right) match {
    case (l: Integer, r: Integer) => l.intValue - r.intValue
    case (l: Utf8, r: Utf8) => l.toString compare r.toString
    case (true, true) => 0
    case (false, true) => -1
    case (true, false) => 1
  }
}

class SimpleExecutor extends QueryExecutor {

  implicit def toOption[A](a: A)= Option(a)

  def apply(plan: QueryPlan)(implicit ctx: Context): QueryIterator = plan match {
    case IndexLookup(namespace, key) => {
      new QueryIterator {
        val name = "SimpleIndexLookup"
        val boundKey = bindKey(namespace, key)
        var result: Option[Record] = None

        def open: Unit =
          result = namespace.get(boundKey)
        def close: Unit =
          result = None

        def hasNext = result.isDefined
        def next = {
          val tuple = Array(boundKey, result.getOrElse(throw new ju.NoSuchElementException("Next on empty iterator")))
          result = None
          tuple
        }
      }
    }
    case IndexScan(namespace, keyPrefix, limit, ascending) => {
        new QueryIterator {
          val name = "SimpleIndexScan"
          val boundKeyPrefix = bindKey(namespace, keyPrefix)
          var result: Seq[(Record, Record)] = Nil
          var pos = 0
          var offset = 0
          var limitReached = false
          val boundLimit = bindLimit(limit)

          @inline private def doFetch() {
            logger.debug("BoundKeyPrefix: %s", boundKeyPrefix)
            result = namespace.getRange(boundKeyPrefix, boundKeyPrefix, offset=offset, limit=boundLimit, ascending=ascending)
            logger.debug("IndexScan Prefetch Returned %s, with offset %d, limit %d", result, offset, boundLimit)
            offset += result.size
            pos = 0
            if (result.size < boundLimit)
              limitReached = true
          }

          def open: Unit = doFetch() 

          def close: Unit =
            result = Nil

          def hasNext = 
            if (pos < result.size) true
            else if (limitReached) false
            else {
              // need to fetch more from KV store to see if we really have more
              doFetch()
              hasNext
            }

          def next = {
            if (!hasNext)
              throw new ju.NoSuchElementException("Next on empty iterator")

            val tuple = Array(result(pos)._1, result(pos)._2)
            pos += 1
            tuple
          }
        }
    }
    case IndexLookupJoin(namespace, key, child) => {
      new QueryIterator {
        val name = "SimpleIndexLookupJoin"
        val childIterator = apply(child)
        var nextTuple: Tuple = null

        def open = {childIterator.open; getNext}
        def close = childIterator.close

        def hasNext = (nextTuple != null)
        def next = {
          val ret = nextTuple
          getNext
          ret
        }

        private def getNext: Unit = {
          while(childIterator.hasNext) {
            val childTuple = childIterator.next
            val boundKey = bindKey(namespace, key, childTuple)
            val value = namespace.get(boundKey)

            if(value.isDefined) {
              nextTuple = childTuple ++ Array[Record](boundKey, value.get)
              return
            }
          }
          nextTuple = null
        }
      }
    }
    case IndexMergeJoin(namespace, keyPrefix, sortFields, limit, ascending, child) => {
      new QueryIterator {
        val name = "SimpleIndexMergeJoin"
        val childIterator = apply(child)
        var tupleBuffers: IndexedSeq[IndexedSeq[Tuple]] = null
        var bufferPos: Array[Int] = null
        var nextTuple: Tuple = null


        def open: Unit = {
          childIterator.open
          tupleBuffers = childIterator.map(childValue => {
            val boundKeyPrefix = bindKey(namespace, keyPrefix, childValue)
            val records = namespace.getRange(boundKeyPrefix, boundKeyPrefix, limit=bindLimit(limit), ascending=ascending)
            logger.debug("IndexMergeJoin Prefetch Using Key %s: %s", boundKeyPrefix, records)
            records.map(r => childValue ++ Array[Record](r._1, r._2)).toIndexedSeq
          }).toIndexedSeq

          bufferPos = Array.fill(tupleBuffers.size)(0)
          getNext
        }
        def close: Unit = childIterator.close

        def hasNext = (nextTuple != null)
        def next = {
          val ret = nextTuple
          getNext
          ret
        }

        private def getNext: Unit = {
          var minIdx = 0
          for(i <- (1 to (tupleBuffers.size - 1))) {
            if((ascending && (compareTuples(tupleBuffers(i)(bufferPos(i)), tupleBuffers(minIdx)(bufferPos(minIdx)), sortFields) < 0)) ||
              (!ascending && (compareTuples(tupleBuffers(i)(bufferPos(i)), tupleBuffers(minIdx)(bufferPos(minIdx)), sortFields) > 0))) {
              minIdx = i
            }
          }
          nextTuple = tupleBuffers(minIdx)(bufferPos(minIdx))
          bufferPos(minIdx) += 1
        }
      }
    }

    case Selection(predicate, child) => {
      new QueryIterator {
        val name = "Selection"
        val childIterator = apply(child)
        var nextTuple: Tuple = null

        def open = {childIterator.open; getNext}
        def close = childIterator.close

        def hasNext = (nextTuple != null)
        def next = {
          val ret = nextTuple
          getNext
          ret
        }

        private def getNext: Unit = {
          while(childIterator.hasNext) {
            val childValue = childIterator.next
            if(evalPredicate(predicate, childValue)) {
              nextTuple = childValue
              return
            }
          }
          nextTuple = null
        }
      }
    }
    case StopAfter(k, child) => {
      new QueryIterator {
        val name = "StopAfter"
        val childIterator = apply(child)
        val limit = bindLimit(k)
        var taken = 0

        def open = {taken = 0; childIterator.open}
        def close = {childIterator.close}

        def hasNext = childIterator.hasNext && (limit > taken)
        def next = {taken += 1; childIterator.next}
      }
    }
  }
}

/**
 * TODO: Should abstract out the common parts between the query iterators.
 */
class ParallelExecutor extends SimpleExecutor {

  override def apply(plan: QueryPlan)(implicit ctx: Context): QueryIterator = plan match {
    case IndexLookup(namespace, key) => {
      new QueryIterator {
        val name = "ParallelIndexLookup"
        val boundKey = bindKey(namespace, key)
        var ftch: Option[ScadsFuture[Option[Record]]] = None 

        def open: Unit =
          ftch = Some(namespace.asyncGet(boundKey))
        def close: Unit =
          ftch = None

        def hasNext =
          ftch.flatMap(_.get).isDefined

        def next = {
          val tuple = Array(boundKey, ftch.flatMap(_.get).getOrElse(throw new ju.NoSuchElementException("Empty iterator")))
          ftch = None
          tuple
        }
      }
    }
    case IndexScan(namespace, keyPrefix, limit, ascending) => {
        new QueryIterator {
          val name = "ParallelIndexScan"
          val boundKeyPrefix = bindKey(namespace, keyPrefix)

          var result: Seq[(Record, Record)] = Nil
          var ftch: ScadsFuture[Seq[(Record, Record)]] = _

          var pos = 0
          var offset = 0
          var limitReached = false
          val boundLimit = bindLimit(limit)
          var ftchInvoked = false

          @inline private def doFetch() {
            logger.debug("BoundKeyPrefix: %s", boundKeyPrefix)
            ftch = namespace.asyncGetRange(boundKeyPrefix, boundKeyPrefix, offset=offset, limit=boundLimit, ascending=ascending)
            ftchInvoked = false
          }

          @inline private def updateFuture() {
            result = ftch.get
            logger.debug("IndexScan Prefetch Returned %s, with offset %d, limit %d", result, offset, boundLimit)
            offset += result.size
            pos = 0
            if (result.size < boundLimit)
              limitReached = true
            ftchInvoked = true
          }

          def open: Unit = doFetch() 

          def close: Unit = {
            result = Nil
            ftch = null
          }

          def hasNext = 
            if (ftchInvoked) { // have we already blocked on ftch and stored the result in result?
              if (pos < result.size) true
              else if (limitReached) false
              else {
                // need to fetch more from KV store to see if we really have more
                doFetch()
                hasNext
              }
            } else {
              updateFuture()
              hasNext
            }

          def next = {
            if (!hasNext)
              throw new ju.NoSuchElementException("Next on empty iterator")

            val tuple = Array(result(pos)._1, result(pos)._2)
            pos += 1
            tuple
          }
        }
    }
    case _ => super.apply(plan)
  }

}
