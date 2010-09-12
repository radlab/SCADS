package edu.berkeley.cs.scads.piql

import org.apache.avro.generic.IndexedRecord

object PhysicalOperators {

  type Namespace = edu.berkeley.cs.scads.storage.Namespace[IndexedRecord, IndexedRecord]
  type KeyGenerator = Seq[Value]

  abstract class Value
  case class FixedValue(v: Any) extends Value

  case class Limit(count: Int)

  trait Predicate
  case class Equality(v1: Value, v2: Value) extends Predicate

  /* Query Plan Nodes */
  abstract class PhysicalOperator

  /* Remote Operators */
  case class IndexLookup(namespace: Namespace, key: KeyGenerator) extends PhysicalOperator
  case class IndexScan(namespace: Namespace, keyPrefix: KeyGenerator, limit: Limit, ascending: Boolean) extends PhysicalOperator
  case class SequentialDereferenceIndex(targetNamespace: Namespace, child: PhysicalOperator) extends PhysicalOperator
  case class PrefixJoin(namespace: Namespace, keyPrefix: KeyGenerator, limit: Limit, ascending: Boolean, child: PhysicalOperator) extends PhysicalOperator
  case class PointerJoin(namespace: Namespace, key: KeyGenerator, child: PhysicalOperator) extends PhysicalOperator

  case class Selection(predicate: Predicate, child: PhysicalOperator) extends PhysicalOperator
  case class Sort(fields: List[String], ascending: Boolean, child: PhysicalOperator) extends PhysicalOperator
  case class TopK(k: Limit, child: PhysicalOperator) extends PhysicalOperator
}
