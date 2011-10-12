package edu.berkeley.cs
package scads.comm
package test

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.Spec
import org.scalatest.matchers.ShouldMatchers

import avro.runtime._
import avro.marker.{AvroUnion, AvroRecord}
import actors.Actor

sealed trait ActorMessages extends AvroUnion
case class ActorMessage1(var f1: Int) extends ActorMessages with AvroRecord
case class ActorMessage2(var f2: Int) extends ActorMessages with AvroRecord

@RunWith(classOf[JUnitRunner])
class RemoteActorSpec extends Spec with ShouldMatchers {
  implicit object ActorRegistry extends ServiceRegistry[ActorMessages]

  class TestActor extends Actor {
    implicit val remoteHandle = ActorRegistry.registerActor(this)
    override def act(): Unit = {
      loop {
        react {
          case Envelope(src, ActorMessage1(x)) =>
            src.foreach(_.asInstanceOf[RemoteService[ActorMessages]] ! ActorMessage2(x))
            //Shutdown and unregister actor
            exit()
            ActorRegistry.unregisterService(remoteHandle)
        }
      }
    }
  }

  describe("RemoteActors") {
    it("should receive external messages") {
      val a = new TestActor
      a.start()

      (a.remoteHandle !! ActorMessage1(1)).get(1000) should equal(Some(ActorMessage2(1)))
    }
  }
}
