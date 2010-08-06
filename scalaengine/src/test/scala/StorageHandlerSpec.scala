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
      handler.remoteHandle !? CreatePartitionRequest("testns", None, None) match {
        case CreatePartitionResponse(newId, newPart) => newPart
        case u => fail("Unexpected message: " + u)
      }
    }

    it("should delete partitions") {
      val handler = getHandler().remoteHandle
      val partId = handler !? CreatePartitionRequest("testns", None, None) match {
        case CreatePartitionResponse(id, service) => id
        case u => fail("Unexpected msg:" + u)
      }

      handler !? DeletePartitionRequest(partId) match {
        case DeletePartitionResponse() => true
        case u => fail("Unexpected response to delete partition: " + u)
      }
    }
  }
}
