package edu.berkeley.cs.scads.test

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.Spec
import org.scalatest.matchers.ShouldMatchers

import edu.berkeley.cs.scads.comm._
import edu.berkeley.cs.scads.storage._

@RunWith(classOf[JUnitRunner])
class PartitionHandlerSpec extends Spec with ShouldMatchers {
  implicit def toOption[A](a: A): Option[A] = Option(a)

  def getHandler(startKey: Option[IntRec] = None, endKey: Option[IntRec] = None) = {
    val handler = TestScalaEngine.getTestHandler()
    val root = handler.root

    root.getOrCreate("namespaces/partitiontestns/keySchema").data = IntRec.schema.toString.getBytes
    root.getOrCreate("namespaces/partitiontestns/valueSchema").data = IntRec.schema.toString.getBytes
    root.getOrCreate("namespaces/partitiontestns/partitions")

    (handler.remoteHandle !? CreatePartitionRequest("partitiontestns", startKey.map(_.toBytes), endKey.map(_.toBytes))) match {
      case CreatePartitionResponse(newPartService) => newPartService
      case _ => fail("Couldn't get partition handler")
    }
  }

  describe("PatitionHandler") {
    it("store values") {
      val partition = getHandler()

      (1 to 10).foreach(i => {
        (partition !? PutRequest(IntRec(i).toBytes, IntRec(i*2).toBytes)) should equal(PutResponse())
      })

      (1 to 10).foreach(i => {
        (partition !? GetRequest(IntRec(i).toBytes)) match {
          case GetResponse(Some(bytes)) => new IntRec().parse(bytes) should equal(IntRec(i*2))
        }
      })
    }

    it("should copy data overwriting existing data") {
      val p1 = getHandler()
      val p2 = getHandler()

      (1 to 10000).foreach(i => p1 !? PutRequest(IntRec(i).toBytes, IntRec(i).toBytes))
      (1 to 5).foreach(i => p2 !? PutRequest(IntRec(i).toBytes, IntRec(i*2).toBytes))

      p2 !? CopyDataRequest(p1, true)

      (1 to 10000).foreach(i => p2 !? GetRequest(IntRec(i).toBytes) match {
        case GetResponse(Some(bytes)) => new IntRec().parse(bytes) should equal(IntRec(i))
        case u => fail("P2 doesn't contain " + i + " instead got: " + u)
      })
    }

    it("should copy data without overwriting existing data") {
      val p1 = getHandler()
      val p2 = getHandler()

      (1 to 10).foreach(i => p1 !? PutRequest(IntRec(i).toBytes, IntRec(i).toBytes))
      (1 to 5).foreach(i => p2 !? PutRequest(IntRec(i).toBytes, IntRec(i*2).toBytes))

      p2 !? CopyDataRequest(p1, false)

      (1 to 5).foreach(i => p2 !? GetRequest(IntRec(i).toBytes) match {
        case GetResponse(Some(bytes)) => new IntRec().parse(bytes) should equal(IntRec(i*2))
        case u => fail("P2 doesn't contain " + i + " instead got: " + u)
      })

      (6 to 10).foreach(i => p2 !? GetRequest(IntRec(i).toBytes) match {
        case GetResponse(Some(bytes)) => new IntRec().parse(bytes) should equal(IntRec(i))
        case u => fail("P2 doesn't contain " + i + " instead got: " + u)
      })
    }

    it("should reject requests which fall outside of its partition range") {
      val p1 = getHandler(IntRec(0), IntRec(10)) // [0, 10)
      val VALUE = IntRec(0).toBytes

      try {
        p1 !? PutRequest(IntRec(-1).toBytes, VALUE) match {
          case u => fail("-1 falls outside of the range of p1's key range: " + u)
        }
      } catch {
        case e: RuntimeException =>
          // supposed to fail
      }

      p1 !? PutRequest(IntRec(0).toBytes, VALUE) match {
        case PutResponse() =>
        case u => fail("0 falls inside of range of p1's key range: " + u)
      }

      p1 !? PutRequest(IntRec(9).toBytes, VALUE) match {
        case PutResponse() =>
        case u => fail("9 falls inside of range of p1's key range: " + u)
      }

      try {
        p1 !? CountRangeRequest(Some(IntRec(1).toBytes), Some(IntRec(11).toBytes)) match {
          case u => fail("11 falls outside of the range of p1's key range: " + u)
        }
      } catch {
        case e: RuntimeException =>
          // supposed to fail
      }

    }

  }
}
