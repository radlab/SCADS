package edu.berkeley.cs
package scads.comm
package test

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.Spec
import org.scalatest.matchers.ShouldMatchers

import avro.runtime._
import avro.marker.{AvroUnion, AvroRecord}

sealed trait TestMessages extends AvroUnion
case class TestMsg1(var f1: Int) extends TestMessages with AvroRecord


@RunWith(classOf[JUnitRunner])
class RemoteServiceSpec extends Spec with ShouldMatchers {
  implicit object TestRegistry extends ServiceRegistry[TestMessages]

  val req = TestMsg1(1)
  val resp = TestMsg1(2)



  object TestService extends ServiceHandler[TestMessages] {
    val logger = net.lag.logging.Logger()
    val registry = TestRegistry
    protected def process(src: Option[RemoteServiceProxy[TestMessages]], msg: TestMessages) = {
      assert(msg == req)
      src.foreach(_ ! resp)
    }
    protected def startup() = null
    protected def shutdown() = null
  }

  describe("RemoteActors") {
    it("should send message asynchronously") {
      val mailbox = new MessageFuture[TestMessages]
      implicit val sender = mailbox.remoteService
      TestService.remoteHandle ! req
      mailbox.get(1000) should equal(Some(resp))
    }

    it("should send messages synchronously") {
      (TestService.remoteHandle !? req) should equal(resp)
    }

    it("should send messages and return a future") {
      (TestService.remoteHandle !! req).get(1000) should equal(Some(resp))
    }
  }
}
