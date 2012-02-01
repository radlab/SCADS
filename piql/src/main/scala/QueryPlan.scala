package edu.berkeley.cs.scads.piql
package plans

import edu.berkeley.cs.scads.storage.client.index._

import collection.JavaConversions._
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, IndexedRecord}

abstract class Value {
  def ===(value: Value) = EqualityPredicate(this, value)
  def like(value: Value) = LikePredicate(this, value)
}

/* Fixed Values.  i.e. Values that arent depended on a specific tuple */
abstract class FixedValue extends Value
case class ConstantValue(v: Any) extends FixedValue
case class ParameterValue(ordinal: Int) extends FixedValue

/* Attribute Values */
case class AttributeValue(recordPosition: Int, fieldPosition: Int) extends Value

case class QualifiedAttributeValue(relation: TupleProvider, field: Field) extends Value {
  def fieldName = field.name
  override def toString = relation.name + "." + field.name
}

case class UnboundAttributeValue(name: String) extends Value {
  protected val qualifiedAttribute = """([^\.]+)\.([^\.]+)""".r
  def relationName: Option[String] = name match {
    case qualifiedAttribute(r,f) => Some(r)
    case _ => None
  }

  def unqualifiedName: String  = name match {
    case qualifiedAttribute(r,f) => f
    case _ => name
  }
}

abstract class Limit
case class FixedLimit(count: Int) extends Limit
case class ParameterLimit(ordinal: Int, max: Int) extends Limit

trait Predicate
case class EqualityPredicate(v1: Value, v2: Value) extends Predicate
case class LikePredicate(v1: Value, v2: Value) extends Predicate
case class InPredicate(v1: Value, v2: Value) extends Predicate

/**
 * An non-leaf operator in a query plan, guaranteed to have children.
 */
abstract trait InnerNode {
  def children: Seq[LogicalPlan]
}

/**
 * An non-leaf operator in a query plan, guaranteed to have children.
 */
abstract trait SingleChildNode extends InnerNode {
  val child: LogicalPlan
  def children: Seq[LogicalPlan] = child :: Nil
}

/* Logical Query Plan nodes */
trait LogicalPlan extends language.Queryable with PlanWalker

/**
 * Project a subset of the fields from the child
 */
case class Project(values: Seq[Value], child: LogicalPlan) extends LogicalPlan

/**
 * Filters child by predicate.
 */
case class Selection(predicate: Predicate, child: LogicalPlan) extends LogicalPlan with SingleChildNode

/**
 * Sorts child by the values specified in attributes.
 */
case class Sort(attributes: Seq[Value], ascending: Boolean, child: LogicalPlan) extends LogicalPlan with SingleChildNode

/**
 * An operator that returns no more than count tuples
 */
trait StopOperator extends SingleChildNode {
  val count: Limit
}

/**
 * Returns tuples page by page, count tuples per page
 */
case class Paginate(count: Limit, child: LogicalPlan) extends LogicalPlan with StopOperator

/**
 * Returns the first count tuples from child.
 */
case class StopAfter(count: Limit, child: LogicalPlan) extends LogicalPlan with StopOperator

/**
 * A promise (i.e. due to external relationship cardinality constraints) that child will return no more than count tuples.
 */
case class DataStopAfter(count: Limit, child: LogicalPlan) extends LogicalPlan with StopOperator

/**
 * Compute the join of the two child query plans.
 */
case class Join(left: LogicalPlan, right: LogicalPlan) extends LogicalPlan with InnerNode {
  def children = Vector(left, right)
}

/**
 * A source of tuples.
 */
trait TupleProvider {
  def schema: Schema
  def keySchema: Schema
  def provider: Namespace
  def name: String

  lazy val keyAttributes = keySchema.getFields.map(f => QualifiedAttributeValue(this, f))
  def attribute(f: String) = QualifiedAttributeValue(this, schema.getField(f))
}

trait Relation extends TupleProvider {
  def index(attrs: Seq[QualifiedAttributeValue]): Index = {
    val remainingKeyFields = keyAttributes.filterNot(attrs contains _)
    Index(attrs ++ remainingKeyFields, this)
  }
}

case class Index(attrs: Seq[QualifiedAttributeValue], relation: Relation) extends TupleProvider {
  def name = "Idx" + relation.name + "(" + attrs.map(_.field.name).mkString(",") + ")"
  lazy val schema =
    Schema.createRecord(
      attrs.map(_.field)
        .zipWithIndex.map { case (f,i) => new Schema.Field(f.name, f.schema, null, null)})

  def keySchema = schema

  // HACK unpackage the index manager and make the index here
  def provider = relation match {
    case r: ScadsRelation =>
      r.ns.getOrCreateIndex(attrs.map(a => AttributeIndex(a.fieldName)))
    case _ =>
      sys.error("Don't know how to create index for this relation")
  }
}

case class ScadsRelation(ns: IndexedNamespace, alias: Option[String] = None) extends Relation with LogicalPlan with TupleProvider {
  def name = alias.getOrElse(ns.name)
  def schema = ns.schema
  def keySchema = ns.keySchema
  def provider = ns
  def as(alias: String) = ScadsRelation(ns, alias)
}

case class ScadsIndex(ns: Namespace) extends LogicalPlan with TupleProvider {
  def name = ns.name
  def schema = ns.schema
  def keySchema = ns.keySchema
  def provider = ns
}

case class LocalTuples(ordinal: Int, alias: String, keySchema: Schema, schema: Schema) extends LogicalPlan with TupleProvider {
  def name = alias
  def provider = null
}

/* Physical Query Plan Nodes */
abstract class QueryPlan
abstract class RemotePlan extends QueryPlan { val namespace: TupleProvider }
abstract trait InnerPlan extends QueryPlan { val child: QueryPlan}

case class IndexLookup(namespace: TupleProvider, key: KeyGenerator) extends QueryPlan

case class IndexScan(namespace: TupleProvider, keyPrefix: KeyGenerator, limit: Limit, ascending: Boolean) extends RemotePlan
case class IndexLookupJoin(namespace: TupleProvider, key: KeyGenerator, child: QueryPlan) extends RemotePlan with InnerPlan
case class IndexScanJoin(namespace: TupleProvider, keyPrefix: KeyGenerator, limit: Limit, ascending: Boolean, child: QueryPlan) extends RemotePlan with InnerPlan
case class IndexMergeJoin(namespace: TupleProvider, keyPrefix: KeyGenerator, sortFields: Seq[Value], limit: Limit, ascending: Boolean, child: QueryPlan) extends RemotePlan with InnerPlan

case class LocalSelection(predicate: Predicate, child: QueryPlan) extends QueryPlan with InnerPlan
case class LocalSort(sortFields: Seq[Value], ascending: Boolean, child: QueryPlan) extends QueryPlan with InnerPlan
case class LocalStopAfter(count: Limit, child: QueryPlan) extends QueryPlan with InnerPlan

/* Testing iterator that simply emits tuples from an iterator that is passed to the query as a parameter */
case class LocalIterator(parameterOrdinal: Int, wrap: Boolean = false) extends QueryPlan


case class Union(child1 : QueryPlan, child2 : QueryPlan, eqField : AttributeValue) extends QueryPlan
