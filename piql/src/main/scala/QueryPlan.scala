package edu.berkeley.cs.scads.piql
package plans

import edu.berkeley.cs.scads.storage.client.index._
import edu.berkeley.cs.scads.storage.GenericNamespace

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

  override def toString = prettyPrint(new StringBuilder).toString

  protected def prettyPrint(sb: StringBuilder, depth: Int = 0): StringBuilder = {
    val S = "  " * depth
    sb.append(S + getClass.getSimpleName + "(\n")
    var first = true
    getClass.getDeclaredFields.foreach(field => {
      field.setAccessible(true)
      if (first)
        first = false
      else
        sb.append(",\n")
      field.get(this) match {
        case in: InnerNode =>
          in.prettyPrint(sb, depth + 1)
        case other =>
          sb.append(S + "  " + other)
      }
    })
    sb.append(")")
    sb
  }
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
 * Project a subset of the fields from the child onto a Schema
 */
case class Project(values: Seq[Value], child: LogicalPlan, schema: Option[Schema]) extends LogicalPlan with SingleChildNode

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
  def index(attrs: Seq[QualifiedAttributeValue]): TupleProvider
}

case class ScadsRelation(ns: IndexedNamespace, alias: Option[String] = None) extends Relation with LogicalPlan with TupleProvider {
  def name = alias.getOrElse(ns.name)
  def schema = ns.schema
  def keySchema = ns.keySchema
  def provider = ns
  def as(alias: String) = ScadsRelation(ns, alias)

  def index(attrs: Seq[QualifiedAttributeValue]): TupleProvider = {
    val remainingKeyFields = keyAttributes.filterNot(attrs contains _)
    val a = attrs ++ remainingKeyFields
    val idx = ns.getOrCreateIndex(attrs.map(a => AttributeIndex(a.fieldName)))
    ScadsIndex(idx)
  }
}

case class ScadsView(ns: IndexNamespace) extends Relation with LogicalPlan {
  def name = ns.name
  def schema = ns.keySchema
  def keySchema = ns.keySchema
  def provider = ns

  def index(attrs: Seq[QualifiedAttributeValue]): TupleProvider = {
    throw new RuntimeException("Should Not Index")
  }
}

case class ScadsIndex(ns: Namespace) extends TupleProvider with LogicalPlan {
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
case class LocalProjection(fields: KeyGenerator, child: QueryPlan, schema: Schema) extends QueryPlan with InnerPlan
case class LocalStopAfter(count: Limit, child: QueryPlan) extends QueryPlan with InnerPlan

/* Testing iterator that simply emits tuples from an iterator that is passed to the query as a parameter */
case class LocalIterator(parameterOrdinal: Int, wrap: Boolean = false) extends QueryPlan
