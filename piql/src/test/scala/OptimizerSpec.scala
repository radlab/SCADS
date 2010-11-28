package edu.berkeley.cs
package scads
package piql
package test

import avro.marker._
import storage.TestScalaEngine

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.Spec
import org.scalatest.matchers.{Matcher, MatchResult, ShouldMatchers}

case class R1(var f1: Int) extends AvroPair
case class R2(var f1: Int, var f2: Int) extends AvroPair

object Relations {
  val cluster = TestScalaEngine.newScadsCluster()
  //TODO: Fix variance of namespace to remove cast
  val r1 = cluster.getNamespace[R1]("r1").asInstanceOf[Namespace]
  val r2 = cluster.getNamespace[R2]("r2").asInstanceOf[Namespace]
}

class OptimizerSpec extends Spec with ShouldMatchers {
  import Relations._

  implicit def opt(logicalPlan: Queryable) = new {
    def opt = Optimizer(logicalPlan)
  }

  describe("The PIQL Optimizer") {
    it("single primary key lookup") {
      val query = r1.where("f1".a === (0.?))
      val plan = IndexLookup(r1, ParameterValue(0) :: Nil)

      query.opt should equal(plan)
    }

    it("composite primary key lookup") {
      val query = (
	r2.where("f1".a === (0.?))
	  .where("f2".a === (1.?)))
      val plan = IndexLookup(r1, ParameterValue(0) :: ParameterValue(1) :: Nil)

      query.opt should equal(plan)
    }
  }

  it("bounded index scan") {
    val query = (
      r2.where("f1".a === (0.?))
	.limit(10))
    val plan = IndexScan(r2,
			 ParameterValue(0) :: Nil,
			 FixedLimit(10),
			 true)
    query.opt should equal(plan)
  }

  it("lookup join") {
    val query = (
      r2.where("f1".a === (0.?))
	.limit(10)
	.join(r1)
	.where("r1.f1".a === "r2.f2".a))
    val plan = IndexLookupJoin(r1, AttributeValue(0, 1) :: Nil,
	         IndexScan(r2, ParameterValue(0) :: Nil, FixedLimit(10), true)
	       )

    query.opt should equal(plan)
  }

  it("local selection") {
    val query = (
      r2.where("f1".a === 0)
	.limit(10)
	.where("f2".a === 0)
      )
    val plan = LocalSelection(AttributeValue(0,1) === 0,
	         IndexScan(r2, ConstantValue(0) :: Nil, FixedLimit(10), true)
	       )

    query.opt should equal(plan)
  }
}
