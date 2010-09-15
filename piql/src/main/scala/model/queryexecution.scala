package edu.berkeley.cs.scads.piql

import org.apache.avro.generic.{GenericData, IndexedRecord}

trait QueryPlans {

  type Namespace = edu.berkeley.cs.scads.storage.GenericNamespace
  type KeyGenerator = Seq[Value]
  type CursorPosition = Seq[Any]
  type Tuple = Array[GenericData.Record]
  type QueryResult = Seq[Tuple]

  abstract class Value
  case class FixedValue(v: Any) extends Value
  case class ParameterValue(ordinal: Int) extends Value
  case class AttributeValue(recordPosition: Int, fieldPosition: Int) extends Value

  abstract class Limit
  case class FixedLimit(count: Int) extends Limit
  case class ParameterLimit(ordinal: Int, max: Int) extends Limit

  trait Predicate
  case class Equality(v1: Value, v2: Value) extends Predicate

  /* Add state var as avro case class */
  case class Context(parameters: Array[Any], state: Option[List[Any]])

  /* Query Plan Nodes */
  abstract class QueryPlan {
    def getIterator(parameters: Array[Any]): QueryIterator = getIterator(Context(parameters, null))
    def getIterator(implicit ctx: Context): QueryIterator

    def bindKey(ns: Namespace, key: KeyGenerator, currentTuple: Tuple = null)(implicit ctx: Context): GenericData.Record = {
      val boundKey = new GenericData.Record(ns.getKeySchema)
      println(key)
      key.map {
        case FixedValue(v) => v
        case ParameterValue(o) => ctx.parameters(o)
        case AttributeValue(recPos, fieldPos) => currentTuple(recPos).get(fieldPos)
      }.zipWithIndex.foreach {
        case (value: Any, idx: Int) => boundKey.put(idx, value)
      }
      boundKey
    }
  }

  abstract class QueryIterator extends Iterator[Tuple] {
    // def open: Unit
    // def close: Unit

    //def serialize: Context
  }

  /* Remote Operators */
  case class IndexLookup(namespace: Namespace, key: KeyGenerator) extends QueryPlan {
    def getIterator(implicit ctx: Context): QueryIterator = {
      val boundKey = bindKey(namespace, key)
      val result = namespace.get(boundKey)
      new QueryIterator {
        var hasNext = result.isDefined
        def next = {
          hasNext = false
          Array(boundKey, result.orNull)
        }
      }
    }
  }

  case class IndexScan(namespace: Namespace, keyPrefix: KeyGenerator, limit: Limit, ascending: Boolean) extends QueryPlan {
    def getIterator(implicit ctx: Context): QueryIterator = throw new RuntimeException("Unimplemented")
  }

  case class SequentialDereferenceIndex(targetNamespace: Namespace, child: QueryPlan) extends QueryPlan {
    def getIterator(implicit ctx: Context): QueryIterator = throw new RuntimeException("Unimplemented")
  }
  case class IndexLookupJoin(namespace: Namespace, key: KeyGenerator, child: QueryPlan) extends QueryPlan {
    def getIterator(implicit ctx: Context): QueryIterator = throw new RuntimeException("Unimplemented")
  }
  case class IndexScanJoin(namespace: Namespace, keyPrefix: KeyGenerator, limit: Limit, ascending: Boolean, child: QueryPlan) extends QueryPlan {
    def getIterator(implicit ctx: Context): QueryIterator = throw new RuntimeException("Unimplemented")
  }
  case class IndexMergeJoin(namespace: Namespace, keyPrefix: KeyGenerator, sortFields: Seq[String], limit: Limit, ascending: Boolean, child: QueryPlan) extends QueryPlan {
    def getIterator(implicit ctx: Context): QueryIterator = throw new RuntimeException("Unimplemented")
  }

  case class Selection(predicate: Predicate, child: QueryPlan) extends QueryPlan {
    def getIterator(implicit ctx: Context): QueryIterator = throw new RuntimeException("Unimplemented")
  }
  case class Sort(fields: List[String], ascending: Boolean, child: QueryPlan) extends QueryPlan {
    def getIterator(implicit ctx: Context): QueryIterator = throw new RuntimeException("Unimplemented")
  }
  case class TopK(k: Limit, child: QueryPlan) extends QueryPlan {
    def getIterator(implicit ctx: Context): QueryIterator = throw new RuntimeException("Unimplemented")
  }
}
