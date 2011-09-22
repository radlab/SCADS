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
      case in: InnerNode => {
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
      case in: InnerNode => {
        val childRes = in.child.gatherUntil(f)
        (f(this) +: childRes._1, childRes._2)
      }
      case leaf => (f(leaf) :: Nil, None)
    }
  }
}

/**
 * An non-leaf node of a query plan, guaranteed to have a child.
 */
abstract trait InnerNode {
  val child: LogicalPlan
}

/**
 * Filters child by predicate.
 */
case class Selection(predicate: Predicate, child: LogicalPlan) extends LogicalPlan with InnerNode

/**
 * Sorts child by the values specified in attributes.
 */
case class Sort(attributes: Seq[Value], ascending: Boolean, child: LogicalPlan) extends LogicalPlan with InnerNode

/**
 * An operator that returns no more than count tuples
 */
trait StopOperator extends InnerNode {
  val count: Limit
}

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
case class Join(left: LogicalPlan, right: LogicalPlan) extends LogicalPlan

/**
 * A source of tuples.
 */
case class Relation(ns: Namespace) extends LogicalPlan
