package edu.berkeley.cs
package scads
package storage
package test

import org.scalatest.{ BeforeAndAfterAll, Spec }
import org.scalatest.junit.JUnitRunner
import org.scalatest.matchers.ShouldMatchers

import org.junit.runner.RunWith

import comm._
import avro.marker._

case class IndexedRecord(var f1: Int) extends AvroPair {
  var f2: String = _
}

@RunWith(classOf[JUnitRunner])
class IndexManagerSpec extends Spec with ShouldMatchers with BeforeAndAfterAll {
  val cluster = TestScalaEngine.newScadsCluster()
  val logger = net.lag.logging.Logger()

  describe("getRange Method") {
    it("implment tokenized indexes") {
      val ns = cluster.getNamespace[IndexedRecord]("tokenTest")
      val idx = ns.getOrCreateIndex(AttributeIndex("f2") :: Nil)
      val tokenIdx = ns.getOrCreateIndex(TokenIndex("f2" :: Nil) :: Nil)
      val rec = IndexedRecord(1)
      rec.f2 = "a b"

      ns.put(rec)
      logger.info("IndexEntries: %s", idx.getRange(None, None))
      idx.lookupRecord("a b", 1).isDefined should equal(true)

      tokenIdx.lookupRecord("a", 1).isDefined should equal(true)
      tokenIdx.lookupRecord("b", 1).isDefined should equal(true)
    }
  }
}
