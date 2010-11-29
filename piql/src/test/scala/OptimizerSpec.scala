package edu.berkeley.cs
package scads
package piql
package test

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.Spec
import org.scalatest.matchers.{Matcher, MatchResult, ShouldMatchers}

class OptimizerSpec extends Spec with ShouldMatchers {
  import Relations._

  implicit def opt(logicalPlan: Queryable) = new {
    def opt = Optimizer(logicalPlan).physicalPlan
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
      val plan = IndexLookup(r2, ParameterValue(0) :: ParameterValue(1) :: Nil)

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
