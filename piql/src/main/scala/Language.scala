package edu.berkeley.cs.scads.piql
package language

import plans._

/**
 * Queryable is the base trait for all objects that can be queried using LINQ style expressions in scala.
 */
trait Queryable {
  self: LogicalPlan =>

  def select(values: Value*) = Project(values, this, None)

  def where(predicate: Predicate) = Selection(predicate, this)

  def join(inner: LogicalPlan) = Join(this, inner)

  def limit(count: Int) = StopAfter(FixedLimit(count), this)

  def limit(parameter: ParameterValue, max: Int) = StopAfter(ParameterLimit(parameter.ordinal, max), this)

  def dataLimit(count: Int) = DataStopAfter(FixedLimit(count), this)

  def sort(attributes: Seq[Value], ascending: Boolean = true) = Sort(attributes, ascending, this)

  def paginate(count: Int) = Paginate(FixedLimit(count), this)
}
