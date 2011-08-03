package edu.berkeley.cs.scads.piql

import org.apache.avro.generic.{GenericData, IndexedRecord}

abstract class Value {
  def ===(value: Value) = EqualityPredicate(this, value)
  def like(value: Value) = LikePredicate(this, value)
}

/* Fixed Values.  i.e. Values that arent depended on a specific tuple */
abstract class FixedValue extends Value
case class ConstantValue(v: Any) extends FixedValue
case class ParameterValue(ordinal: Int) extends FixedValue

/* Attibute Values */
case class AttributeValue(recordPosition: Int, fieldPosition: Int) extends Value
case class UnboundAttributeValue(name: String) extends Value

abstract class Limit
case class FixedLimit(count: Int) extends Limit
case class ParameterLimit(ordinal: Int, max: Int) extends Limit

trait Predicate
case class EqualityPredicate(v1: Value, v2: Value) extends Predicate
case class LikePredicate(v1: Value, v2: Value) extends Predicate
case class InPredicate(v1: Value, v2: Value) extends Predicate

/* Physical Query Plan Nodes */
abstract class QueryPlan
abstract class RemotePlan extends QueryPlan { val namespace: Namespace }

case class IndexLookup(namespace: Namespace, key: KeyGenerator) extends QueryPlan

case class IndexScan(namespace: Namespace, keyPrefix: KeyGenerator, limit: Limit, ascending: Boolean) extends RemotePlan
case class IndexLookupJoin(namespace: Namespace, key: KeyGenerator, child: QueryPlan) extends RemotePlan
case class IndexScanJoin(namespace: Namespace, keyPrefix: KeyGenerator, limit: Limit, ascending: Boolean, child: QueryPlan) extends RemotePlan
case class IndexMergeJoin(namespace: Namespace, keyPrefix: KeyGenerator, sortFields: Seq[Value], limit: Limit, ascending: Boolean, child: QueryPlan) extends RemotePlan

case class LocalSelection(predicate: Predicate, child: QueryPlan) extends QueryPlan
case class LocalSort(sortFields: Seq[Value], ascending: Boolean, child: QueryPlan) extends QueryPlan
case class LocalStopAfter(count: Limit, child: QueryPlan) extends QueryPlan

/* Testing iterator that simply emits tuples from an iterator that is passed to the query as a parameter */
case class LocalIterator(parameterOrdinal: Int, wrap: Boolean = false) extends QueryPlan


case class Union(child1 : QueryPlan, child2 : QueryPlan, eqField : AttributeValue) extends QueryPlan
