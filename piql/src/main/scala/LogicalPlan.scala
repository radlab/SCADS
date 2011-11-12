package edu.berkeley.cs
package scads
package piql
package plans

import language.Queryable
import javax.xml.soap.SOAPElementFactory

/**
 * A node in a logical query plan.
 */
trait PlanWalker {
  self: LogicalPlan =>
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

  def flatGather[A](f: LogicalPlan => Seq[A]): Seq[A] = this match {
    case in: SingleChildNode =>
      in.child.flatGather(f) ++ f(in)
    case leaf => f(leaf)
  }
}



