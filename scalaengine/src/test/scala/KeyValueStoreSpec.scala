//package edu.berkeley.cs.scads.test
//
//import org.junit.runner.RunWith
//import org.scalatest.junit.JUnitRunner
//import org.scalatest.Spec
//import org.scalatest.matchers.ShouldMatchers
//
//import edu.berkeley.cs.scads.comm._
//import edu.berkeley.cs.scads.comm.Conversions._
//import edu.berkeley.cs.scads.storage._
//import com.googlecode.avro.marker.AvroRecord
//
//case class IntRec(var f1: Int) extends AvroRecord
//case class StringRec(var f1: String) extends AvroRecord
//
//@RunWith(classOf[JUnitRunner])
//class KeyValueStoreSpec extends Spec with ShouldMatchers {
//  val cluster = TestScalaEngine.cluster
//
//  implicit def toOption[A](a: A): Option[A] = Option(a)
//
//  describe("SCADS Namespace") {
//    it("should implement get/put") {
//      val ns = cluster.getNamespace[IntRec, StringRec]("getputtest")
//
//      (1 to 100).foreach(i => ns.put(IntRec(i), StringRec(i.toString)))
//      (1 to 100).foreach(i => ns.get(IntRec(i)) should equal(Some(StringRec(i.toString))))
//    }
//
//    it("should delete data") {
//      val ns = cluster.getNamespace[IntRec, StringRec]("deletetest")
//
//      /* Insert Integers 1-100 */
//      (1 to 100).foreach(i => ns.put(IntRec(i), StringRec(i.toString)))
//      /* Delete all odd records */
//      (1 to 100 by 2).foreach(i => ns.put(IntRec(i), None))
//      /* Make sure even ones are still there and odd ones are gone */
//      (1 to 100).foreach(i => ns.get(IntRec(i)) should equal(if(i % 2 == 0) Some(StringRec(i.toString)) else None))
//      /* Make sure gaps aren't left when we getRange over deleted data */
//      ns.getRange(None, None).map(_._1.f1) should equal(2 to 100 by 2)
//    }
//
//    describe("getRange Method") {
//      it("should correctly return ranges") {
//        val ns = cluster.getNamespace[IntRec, IntRec]("rangetest")
//
//        /* Insert Integers 1-100 */
//        (1 to 100).foreach(i => ns.put(IntRec(i),IntRec(i)))
//
//        /* Various range checks */
//        ns.getRange(None, None).map(_._1.f1)               should equal(1 to 100)
//        ns.getRange(None, IntRec(50)).map(_._1.f1)         should equal(1 to 50)
//        ns.getRange(IntRec(50), None).map(_._1.f1)         should equal(50 to 100)
//        ns.getRange(IntRec(10), IntRec(90)).map(_._1.f1)   should equal(10 to 90)
//        ns.getRange(IntRec(-10), None).map(_._1.f1)        should equal(1 to 100)
//        ns.getRange(None, IntRec(110)).map(_._1.f1)        should equal(1 to 100)
//        ns.getRange(IntRec(-10), IntRec(110)).map(_._1.f1) should equal(1 to 100)
//      }
//
//
//      it("should correctly return ranges obeying the limit") {
//        val ns = cluster.getNamespace[IntRec, IntRec]("rangetestlimit")
//
//        /* Insert Integers 1-100 */
//        (1 to 100).foreach(i => ns.put(IntRec(i),IntRec(i)))
//
//        /* Various range checks */
//        ns.getRange(None, None, limit=10).map(_._1.f1)               should equal(1 to 10)
//        ns.getRange(None, IntRec(50), limit=10).map(_._1.f1)         should equal(1 to 10)
//        ns.getRange(IntRec(50), None, limit=10).map(_._1.f1)         should equal(50 to 60)
//        ns.getRange(IntRec(10), IntRec(90), limit=10).map(_._1.f1)   should equal(10 to 20)
//        ns.getRange(IntRec(-10), None, limit=10).map(_._1.f1)        should equal(1 to 10)
//        ns.getRange(None, IntRec(110), limit=10).map(_._1.f1)        should equal(1 to 10)
//        ns.getRange(IntRec(-10), IntRec(110), limit=10).map(_._1.f1) should equal(1 to 10)
//      }
//
//      it("should correctly return ranges backwards") {
//        val ns = cluster.getNamespace[IntRec, IntRec]("rangetestbackwards")
//
//        /* Insert Integers 1-100 */
//        (1 to 100).foreach(i => ns.put(IntRec(i),IntRec(i)))
//
//        /* Various range checks */
//        ns.getRange(None, None, backwards=true).map(_._1.f1)               should equal(100 to 1)
//        ns.getRange(None, IntRec(50), backwards=true).map(_._1.f1)         should equal(50 to 1)
//        ns.getRange(IntRec(50), None, backwards=true).map(_._1.f1)         should equal(60 to 50)
//        ns.getRange(IntRec(10), IntRec(90), backwards=true).map(_._1.f1)   should equal(90 to 10)
//        ns.getRange(IntRec(-10), None, backwards=true).map(_._1.f1)        should equal(100 to 1)
//        ns.getRange(None, IntRec(110), backwards=true).map(_._1.f1)        should equal(100 to 1)
//        ns.getRange(IntRec(-10), IntRec(110), backwards=true).map(_._1.f1) should equal(100 to 1)
//      }
//
//      it("should correctly return ranges backwards with limit") {pending}
//    }
//
//    it("should implement test/set") {pending}
//    it("should correctly return a count of records") {pending}
//    it("should handle schema resolution automatically") {pending}
//    it("should correctly return records by prefix") {pending}
//    it("should allow data to be moved/copied") {pending}
//    it("should efficently bulk load from iterator using ++=") {pending}
//    it("should allow you to truncate the contents of a namespace") {pending}
//    it("should automatically handle schema resolution") {pending}
//
//    it("should implement closure-shipping version of flatMap") {pending}
//  }
//}
