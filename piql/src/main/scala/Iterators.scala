package edu.berkeley.cs.scads.piql

import org.apache.avro.generic.{GenericData, IndexedRecord}

case class Context(parameters: Array[Any], state: Option[List[Any]])

trait QueryExecutor {
  abstract class QueryIterator extends Iterator[Tuple] {
    def open: Unit
    def close: Unit
  }
 
  def apply(plan: QueryPlan, args: Any*): QueryIterator = apply(plan)(Context(args.toArray, None))
  def apply(plan: QueryPlan)(implicit ctx: Context): QueryIterator

  protected def bindKey(ns: Namespace, key: KeyGenerator, currentTuple: Tuple = null)(implicit ctx: Context): GenericData.Record = {
    val boundKey = ns.newKeyInstance
    key.map {
      case FixedValue(v) => v
      case ParameterValue(o) => ctx.parameters(o)
      case AttributeValue(recPos, fieldPos) => currentTuple(recPos).get(fieldPos)
    }.zipWithIndex.foreach {
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
}

object SimpleExecutor extends QueryExecutor {
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
          result = namespace.getRange(Some(boundKeyPrefix), Some(boundKeyPrefix), limit=bindLimit(limit), ascending=ascending)
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
  case SequentialDereferenceIndex(targetNamespace, child) => throw new RuntimeException("Not Implemented")
  case IndexLookupJoin(namespace, key, child) => throw new RuntimeException("Not Implemented")
  case IndexScanJoin(namespace, keyPrefix, limit, ascending, child) => throw new RuntimeException("Not Implemented")
  case IndexMergeJoin(namespace, keyPrefix, sortFields, limit, ascending, child) => throw new RuntimeException("Not Implemented")

  case Selection(predicate, child) => throw new RuntimeException("Not Implemented")
  case Sort(fields, ascending, child) => throw new RuntimeException("Not Implemented")
  case StopAfter(k, child) => throw new RuntimeException("Not Implemented")
  }
}
