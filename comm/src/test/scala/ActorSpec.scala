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

  val msg = TestMsg1(1)

  object EchoService extends ServiceHandler[TestMessages] {
    val logger = net.lag.logging.Logger()
    val registry = TestRegistry
    protected def process(src: Option[RemoteServiceProxy[TestMessages]], msg: TestMessages) = src.foreach(_ ! msg)
    protected def startup() = null
    protected def shutdown() = null
  }

  describe("RemoteActors") {
    it("should send message asynchronously") {
      val mailbox = new MessageFuture[TestMessages]
      implicit val sender = mailbox.remoteService
      mailbox.remoteService ! msg
      mailbox.get(1000) should equal(Some(msg))
    }

    it("should send messages synchronously") {
      (EchoService.remoteHandle !? msg) should equal(msg)
    }

    it("should send messages and return a future") {
      (EchoService.remoteHandle !! msg).get(1000) should equal(Some(msg))
    }
  }
}
