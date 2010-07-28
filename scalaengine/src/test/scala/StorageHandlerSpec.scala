package edu.berkeley.cs.scads.test

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.Spec
import org.scalatest.matchers.ShouldMatchers

import edu.berkeley.cs.scads.comm._
import edu.berkeley.cs.scads.storage._

@RunWith(classOf[JUnitRunner])
class StorageHandlerSpec extends Spec with ShouldMatchers {
  implicit def toOption[A](a: A): Option[A] = Option(a)

  def getHandler() = {
    val handler = TestScalaEngine.getTestHandler()
    val root = handler.root

    root.getOrCreate("namespaces/testns/keySchema").data = IntRec.schema.toString.getBytes
    root.getOrCreate("namespaces/testns/valueSchema").data = IntRec.schema.toString.getBytes
    root.getOrCreate("namespaces/testns/partitions")

    handler
  }

  describe("StorageHandler") {
    it("should create partitions") {
      val handler = getHandler()
      handler.remoteHandle !? CreatePartitionRequest("testns", "1", None, None) match {
        case CreatePartitionResponse(newPart) => newPart
        case u => fail("Unexpected message: " + u)
      }
    }

    it("should fail to create duplicate partitions") {
      val handler = getHandler()
      handler.remoteHandle !? CreatePartitionRequest("testns", "1", None, None) match {
        case CreatePartitionResponse(newPart) => newPart
        case u => fail("Unexpected message: " + u)
      }

      evaluating {
        handler.remoteHandle !? CreatePartitionRequest("testns", "1", None, None)
      } should produce[Exception]
    }

    it("should delete partitions") {
      val handler = getHandler().remoteHandle
      handler !? CreatePartitionRequest("testns", "1", None, None)
      handler !? DeletePartitionRequest("1") match {
        case DeletePartitionResponse() => true
        case u => fail("Unexpected response to delete partition: " + u)
      }
    }

    it("should merge partitions") {
      val handler = getHandler().remoteHandle
      val p1 = handler !? CreatePartitionRequest("testns", "1", None, IntRec(5).toBytes) match {
        case CreatePartitionResponse(p) => p
        case _ => fail("Unexpected response to create partition")
      }
      val p2 = handler !? CreatePartitionRequest("testns", "2", IntRec(5).toBytes, None) match {
        case CreatePartitionResponse(p) => p
        case _ => fail("Unexpected response to create partition")
      }

      (1 to 4).foreach(i => p1 !? PutRequest(IntRec(i).toBytes, IntRec(i).toBytes))
      (5 to 10).foreach(i => p2 !? PutRequest(IntRec(i).toBytes, IntRec(i).toBytes))

      val mergedPartition = handler !? MergePartitionRequest("1", "2") match {
        case MergePartitionResponse(newp) => newp
        case _ => fail("Unexpected response to MergePartition")
      }

      (1 to 10).foreach(i => mergedPartition !? GetRequest(IntRec(i).toBytes) match {
        case GetResponse(Some(bytes)) => new IntRec().parse(bytes) should equal(IntRec(i))
        case u => fail("Merged partition doesn't contain " + i + " instead got: " + u)
      })
    }
  }
}
