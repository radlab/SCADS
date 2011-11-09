package edu.berkeley.cs
package scads
package piql
package test

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.Spec
import org.scalatest.matchers.{Matcher, MatchResult, ShouldMatchers}

import opt._
import plans._
import language._

import storage.client.index._

class OptimizerSpec extends Spec with ShouldMatchers {

  import Relations._

  implicit def opt(logicalPlan: LogicalPlan) = new {
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

  it("bounded primary index scan") {
    val query = (
      r2.where("f1".a === (0.?))
        .limit(10))
    val plan = LocalStopAfter(FixedLimit(10),
      IndexScan(r2,
        ParameterValue(0) :: Nil,
        FixedLimit(10),
        true))

    query.opt should equal(plan)
  }

  it("bounded primary index scan attr limit") {
    val query = (
      r2.where("f1".a === (0.?))
        .limit(1.?, 10))
    val plan = LocalStopAfter(ParameterLimit(1, 10),
      IndexScan(r2,
        ParameterValue(0) :: Nil,
        ParameterLimit(1, 10),
        true))

    query.opt should equal(plan)
  }

  it("lookup join") {
    val query = (
      r2.where("f1".a === (0.?))
        .dataLimit(10)
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
        .dataLimit(10)
        .where("f2".a === 0)
      )
    val plan = LocalSelection(AttributeValue(0, 1) === 0,
      IndexScan(r2, ConstantValue(0) :: Nil, FixedLimit(10), true)
    )

    query.opt should equal(plan)
  }

  it("bounded index query") {
    val query = (
      r2.where("f2".a === 0)
        .limit(10)
      )

    //Optimize query first so index is created
    val optQuery = query.opt
    val idx = r2.getOrCreateIndex(AttributeIndex("f2") :: Nil)

    val plan =
      LocalStopAfter(FixedLimit(10),
        IndexLookupJoin(r2, AttributeValue(0, 1) :: AttributeValue(0, 0) :: Nil,
          IndexScan(idx, ConstantValue(0) :: Nil, FixedLimit(10), true)))

    optQuery should equal(plan)
  }

  it("simple merge sort join") {
    val query = (
      r2.where("f1".a === 0)
        .dataLimit(5)
        .join(r2Prime)
        .where("r2.f2".a === "r2Prime.f1".a)
        .sort("r2Prime.f2".a :: Nil)
        .limit(10)
      )
    val plan =
      LocalStopAfter(FixedLimit(10),
        IndexMergeJoin(r2Prime,
          AttributeValue(0, 1) :: Nil,
          AttributeValue(1, 1) :: Nil,
          FixedLimit(10),
          true,
          IndexScan(r2, ConstantValue(0) :: Nil, FixedLimit(5), true)))

    query.opt should equal(plan)
  }
}
