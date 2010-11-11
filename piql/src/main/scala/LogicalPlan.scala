package edu.berkeley.cs
package scads
package piql

trait Queryable {
  def where(predicate: Predicate) = Selection(predicate, this)
  def join(inner: Queryable) = Join(this, inner)
  def limit(count: Int) = StopAfter(count, this)
  def sort(attributes: Seq[Value], ascending: Boolean = true) = Sort(attributes, ascending, this)
}

case class Selection(predicate: Predicate, child: Queryable) extends Queryable
case class Sort(attributes: Seq[Value], ascending: Boolean, child: Queryable) extends Queryable
case class Join(left: Queryable, right: Queryable) extends Queryable
case class StopAfter(count: Int, child: Queryable) extends Queryable
case class Relation(ns: Namespace) extends Queryable
