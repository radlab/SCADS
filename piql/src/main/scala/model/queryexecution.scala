package edu.berkeley.cs.scads.piql

import org.apache.avro.generic.IndexedRecord

class PhysicalOperators {

  type Namespace = edu.berkeley.cs.scads.storage.Namespace[IndexedRecord, IndexedRecord]
  type KeyGenerator = Seq[Value]
  type QueryResult = IndexedRecord

  abstract class Value
  case class FixedValue(v: Any) extends Value
  case class ParameterValue(ordinal: Int) extends Value
  case class AttributeValue(attrName: String) extends Value

  abstract class Limit
  case class FixedLimit(count: Int) extends Limit
  case class ParameterLimit(ordinal: Int, max: Int) extends Limit

  trait Predicate
  case class Equality(v1: Value, v2: Value) extends Predicate

  /* Query Plan Nodes */
  abstract class PhysicalOperator {
    def execute(params: Array[Any]): QueryResult = throw new RuntimeException("Unimplemented")
  }

  /* Remote Operators */
  case class IndexLookup(namespace: Namespace, key: KeyGenerator) extends PhysicalOperator
  case class IndexScan(namespace: Namespace, keyPrefix: KeyGenerator, limit: Limit, ascending: Boolean) extends PhysicalOperator
  case class SequentialDereferenceIndex(targetNamespace: Namespace, child: PhysicalOperator) extends PhysicalOperator
  case class IndexScanJoin(namespace: Namespace, keyPrefix: KeyGenerator, limit: Limit, ascending: Boolean, child: PhysicalOperator) extends PhysicalOperator
  case class IndexLookupJoin(namespace: Namespace, key: KeyGenerator, child: PhysicalOperator) extends PhysicalOperator
  case class IndexMergeJoin(namespace: Namespace, key: KeyGenerator, sortFields: Seq[String], limit: Limit, ascending: Boolean, child: PhysicalOperator) extends PhysicalOperator

  case class Selection(predicate: Predicate, child: PhysicalOperator) extends PhysicalOperator
  case class Sort(fields: List[String], ascending: Boolean, child: PhysicalOperator) extends PhysicalOperator
  case class TopK(k: Limit, child: PhysicalOperator) extends PhysicalOperator
}
