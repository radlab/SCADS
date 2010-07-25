package edu.berkeley.cs.scads.test

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.Spec
import org.scalatest.matchers.ShouldMatchers

import edu.berkeley.cs.scads.comm._
import edu.berkeley.cs.scads.storage._

@RunWith(classOf[JUnitRunner])
class PartitionHandlerSpec extends Spec with ShouldMatchers {
  val currentPartitionId = new java.util.concurrent.atomic.AtomicInteger
  implicit def toOption[A](a: A): Option[A] = Option(a)

  def getHandler() = {
    val handler = TestScalaEngine.getTestHandler()
    val root = handler.root

    root.getOrCreate("namespaces/partitiontestns/keySchema").data = IntRec.schema.toString.getBytes
    root.getOrCreate("namespaces/partitiontestns/valueSchema").data = IntRec.schema.toString.getBytes
    root.getOrCreate("namespaces/partitiontestns/partitions")

    (handler.remoteHandle !? CreatePartitionRequest("partitiontestns", currentPartitionId.getAndIncrement.toString, None, None)) match {
      case CreatePartitionResponse(newPart) => newPart
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
        (partition !? GetRequest(IntRec(i).toBytes)) should equal(GetResponse(Some(IntRec(i*2).toBytes)))
      })
    }
  }
}
