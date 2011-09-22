package edu.berkeley.cs
package scads
package piql
package plans

trait Queryable {
  def where(predicate: Predicate) = Selection(predicate, this)
  def join(inner: Queryable) = Join(this, inner)
  def limit(count: Int) = StopAfter(FixedLimit(count), this)
  def limit(parameter: ParameterValue, max: Int) = StopAfter(ParameterLimit(parameter.ordinal, max), this)
  def dataLimit(count: Int) = DataStopAfter(FixedLimit(count), this)
  def sort(attributes: Seq[Value], ascending: Boolean = true) = Sort(attributes, ascending, this)

  def walkPlan[A](f: Queryable => A): A = {
    this match {
      case in: InnerNode => {
	f(this)
	in.child.walkPlan(f)
      }
      case leaf => f(leaf)
    }
  }

  def gatherUntil[A](f: PartialFunction[Queryable, A]): (Seq[A], Option[Queryable]) = {
    if(f.isDefinedAt(this) == false)
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

abstract trait InnerNode {
  val child: Queryable
}

case class Selection(predicate: Predicate, child: Queryable) extends Queryable with InnerNode
case class Sort(attributes: Seq[Value], ascending: Boolean, child: Queryable) extends Queryable with InnerNode

trait StopOperator extends InnerNode {
  val count: Limit
}
case class StopAfter(count: Limit, child: Queryable) extends Queryable with StopOperator
case class DataStopAfter(count: Limit, child: Queryable) extends Queryable with StopOperator

case class Join(left: Queryable, right: Queryable) extends Queryable

case class Relation(ns: Namespace) extends Queryable
