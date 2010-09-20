package edu.berkeley.cs.scads.piql

import org.apache.avro.util.Utf8
import org.apache.avro.generic.{GenericData, IndexedRecord}

case class Context(parameters: Array[Any], state: Option[List[Any]])

trait QueryExecutor {
  abstract class QueryIterator extends Iterator[Tuple] {
    def open: Unit
    def close: Unit
  }

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
    case ParameterLimit(lim, max) =>
      if(lim <= max)
        lim
      else
        throw new RuntimeException("Limit out of range")
  }

  protected def evalPredicate(predicate: Predicate, tuple: Tuple)(implicit ctx: Context): Boolean = predicate match {
    case Equality(v1, v2) => compareAny(bindValue(v1, tuple), bindValue(v2, tuple)) == 0
  }

  protected def compareRecords(left: Record, right: Record, fields: Seq[String]): Int = {
    fields.foreach(field => {
      val leftValue = left.get(left.getSchema.getField(field).pos)
      val rightValue = left.get(left.getSchema.getField(field).pos)

      val comparison = compareAny(leftValue, rightValue)
      if(comparison != 0)
        return comparison
    })
    return 0
  }

  protected def compareAny(left: Any, right: Any): Int = (left, right) match {
    case (l: Integer, r: Integer) => l.intValue - r.intValue
    case (l: Utf8, r: Utf8) => l.toString compare r.toString
  }
}

object SimpleExecutor extends QueryExecutor {

  implicit def toOption[A](a: A)= Option(a)

  def apply(plan: QueryPlan)(implicit ctx: Context): QueryIterator = plan match {
    case IndexLookup(namespace, key) => {
      new QueryIterator {
        val boundKey = bindKey(namespace, key)
        var result: Option[Record] = None

        def open: Unit =
          result = namespace.get(boundKey)
        def close: Unit =
          result = None

        def hasNext = result.isDefined
        def next = {
          val tuple = Array(boundKey, result.getOrElse(throw new java.util.NoSuchElementException("Next on empty iterator")))
          result = None
          tuple
        }
      }
    }
    case IndexScan(namespace, keyPrefix, limit, ascending) => {
        new QueryIterator {
          val boundKeyPrefix = bindKey(namespace, keyPrefix)
          var result: Seq[(Record, Record)] = Nil
          var pos = 0

          def open: Unit =
            result = namespace.getRange(boundKeyPrefix, boundKeyPrefix, limit=bindLimit(limit), ascending=ascending)
          def close: Unit =
            result = Nil

          def hasNext = pos < result.size
          def next = {
            if(pos >= result.size)
              throw new java.util.NoSuchElementException("Next on empty iterator")

            val tuple = Array(result(pos)._1, result(pos)._2)
            pos += 1
            tuple
          }
        }
    }
    case SequentialDereferenceIndex(targetNamespace, child) => {
      new QueryIterator {
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
            val value = targetNamespace.get(childTuple.last)
            if(value.isDefined) {
              nextTuple = childTuple ++ Array[Record](childTuple.last, value.get)
              return
            }
          }
          nextTuple = null
        }
      }
    }
    case IndexLookupJoin(namespace, key, child) => {
      new QueryIterator {
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
        val childIterator = apply(child)
        var childValues: IndexedSeq[Tuple] = null
        var recordBuffers: IndexedSeq[Seq[(Record, Record)]] = null
        var bufferPos: Array[Int] = null
        var nextTuple: Tuple = null


        def open: Unit = {
          childIterator.open
          childValues = childIterator.toIndexedSeq
          recordBuffers = childValues.map(childValue => {
            val boundKeyPrefix = bindKey(namespace, keyPrefix, childValue)
            namespace.getRange(boundKeyPrefix, boundKeyPrefix, limit=bindLimit(limit), ascending=ascending)
          })
          bufferPos = Array.fill(recordBuffers.size)(0)
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
          for(i <- (1 to recordBuffers.size))
            if(compareRecords(recordBuffers(i)(bufferPos(i))._1, recordBuffers(minIdx)(bufferPos(minIdx))._1, sortFields) < 0)
              minIdx = i

          nextTuple = childValues(minIdx) ++ Array(recordBuffers(minIdx)(bufferPos(minIdx))._1, recordBuffers(minIdx)(bufferPos(minIdx))._2)
          bufferPos(minIdx) += 1
        }
      }
    }

    case Selection(predicate, child) => {
      new QueryIterator {
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
        val childIterator = apply(child)
        val limit = bindLimit(k)
        var taken = 0

        def open = {taken = 0; childIterator.open}
        def close = {childIterator.close}

        def hasNext = childIterator.hasNext && (limit < taken)
        def next = {taken += 1; childIterator.next}
      }
    }
  }
}
