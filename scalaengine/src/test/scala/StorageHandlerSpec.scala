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

  describe("StorageHandler") {
    it("should create partitions") {
      val handler = TestScalaEngine.getTestHandler()
      val root = handler.root

      root.getOrCreate("namespaces/testns/keySchema").data = IntRec.schema.toString.getBytes
      root.getOrCreate("namespaces/testns/valueSchema").data = IntRec.schema.toString.getBytes
      root.getOrCreate("namespaces/testns/partitions")

      val newPartition = handler.remoteHandle !? CreatePartitionRequest("testns", "1", None, None) match {
        case CreatePartitionResponse(newPart) => newPart
        case u => fail("Unexpected message: " + u)
      }
    }
  }
}
