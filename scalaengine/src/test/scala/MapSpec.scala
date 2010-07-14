package edu.berkeley.cs.scads.test

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.Spec
import org.scalatest.matchers.ShouldMatchers

import edu.berkeley.cs.scads.comm._
import edu.berkeley.cs.scads.comm.Conversions._
import edu.berkeley.cs.scads.storage._
import com.googlecode.avro.marker.AvroRecord

case class IntRec(var f1: Int) extends AvroRecord

@RunWith(classOf[JUnitRunner])
class NamespaceSpec extends Spec with ShouldMatchers {

  describe("SCADS Namespace") {
    it("should implement get/put") {
      val intRec = new IntRec(1)
      val gptest = TestScalaEngine.cluster.getNamespace[IntRec, IntRec]("getputtest")
      val (ir1, ir2) = (IntRec(1234), IntRec(5478))

      gptest.put(ir1, Some(ir2))
      gptest.get(ir1) should equal(Some(ir2))
    }

    it("should implement test/set") {pending}
    it("should correctly return a count of records") {pending}
    it("should handle schema resolution automatically") {pending}
    it("should delete data") {pending}
    it("should correctly return ranges of data") {pending}
    it("should correctly return records by prefix") {pending}
    it("should allow data to be moved/copied") {pending}
    it("should efficently bulk load from iterator using ++=") {pending}
    it("should allow you to truncate the contents of a namespace") {pending}
  }
}
