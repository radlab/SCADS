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
  }
}
