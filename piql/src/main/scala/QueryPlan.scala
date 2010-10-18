package edu.berkeley.cs.scads.piql

import org.apache.avro.generic.{GenericData, IndexedRecord}

abstract class Value
case class FixedValue(v: Any) extends Value
case class ParameterValue(ordinal: Int) extends Value
case class AttributeValue(recordPosition: Int, fieldPosition: Int) extends Value

abstract class Limit
case class FixedLimit(count: Int) extends Limit
case class ParameterLimit(ordinal: Int, max: Int) extends Limit

trait Predicate
case class Equality(v1: Value, v2: Value) extends Predicate

/* Query Plan Nodes */
abstract class QueryPlan
case class IndexLookup(namespace: Namespace, key: KeyGenerator) extends QueryPlan
case class IndexScan(namespace: Namespace, keyPrefix: KeyGenerator, limit: Limit, ascending: Boolean) extends QueryPlan
case class IndexLookupJoin(namespace: Namespace, key: KeyGenerator, child: QueryPlan) extends QueryPlan
case class IndexScanJoin(namespace: Namespace, keyPrefix: KeyGenerator, limit: Limit, ascending: Boolean, child: QueryPlan) extends QueryPlan
case class IndexMergeJoin(namespace: Namespace, keyPrefix: KeyGenerator, sortFields: Seq[AttributeValue], limit: Limit, ascending: Boolean, child: QueryPlan) extends QueryPlan
case class Selection(predicate: Predicate, child: QueryPlan) extends QueryPlan
case class Sort(sortFields: Seq[AttributeValue], ascending: Boolean, child: QueryPlan) extends QueryPlan
case class StopAfter(count: Limit, child: QueryPlan) extends QueryPlan

case class Union(child1 : QueryPlan, child2 : QueryPlan, eqField : AttributeValue) extends QueryPlan

