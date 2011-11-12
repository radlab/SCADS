package edu.berkeley.cs
package scads
package piql
package plans

import language.Queryable

/**
 * A node in a logical query plan.
 */
abstract class LogicalPlan extends Queryable {
  def walkPlan[A](f: LogicalPlan => A): A = {
    this match {
      case in: SingleChildNode => {
        f(this)
        in.child.walkPlan(f)
      }
      case leaf => f(leaf)
    }
  }

  def gatherUntil[A](f: PartialFunction[LogicalPlan, A]): (Seq[A], Option[LogicalPlan]) = {
    if (f.isDefinedAt(this) == false)
      return (Nil, Some(this))

    this match {
      case in: SingleChildNode => {
        val childRes = in.child.gatherUntil(f)
        (f(this) +: childRes._1, childRes._2)
      }
      case leaf => (f(leaf) :: Nil, None)
    }
  }
}



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

/**
 * Project a subset of the fields from the child
 */
case class Project(values: Seq[Value], child: LogicalPlan) extends LogicalPlan with SingleChildNode

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
}
case class Relation(ns: IndexedNamespace, alias: Option[String] = None) extends LogicalPlan with TupleProvider{
  def schema = ns.schema
  def keySchema = ns.keySchema
  def provider = ns
}

case class Index(ns: Namespace) extends LogicalPlan with TupleProvider {
  def schema = ns.schema
  def keySchema = ns.keySchema
  def provider = ns
}
