package edu.berkeley.cs.scads.comm.test

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.Spec
import org.scalatest.matchers.ShouldMatchers

import edu.berkeley.cs.scads.comm._

@RunWith(classOf[JUnitRunner])
class RemoteActorSpec extends Spec with ShouldMatchers {
  val msg = PutResponse()

  describe("RemoteActors") {
    it("should send message asynchronously") {
      val mailbox = new MessageFutureImpl
      implicit val sender = mailbox.remoteActor
      mailbox.remoteActor ! msg
      mailbox.get(1000) should equal(Some(msg))
    }

    it("should send messages synchronously") {
      (EchoActor.remoteActor !? msg) should equal(msg)
    }

    it("should send messages and return a future") {
      (EchoActor.remoteActor !! msg).get(1000) should equal(Some(msg))
    }
  }
}
