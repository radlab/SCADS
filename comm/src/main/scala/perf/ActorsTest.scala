package edu.berkeley.cs
package scads
package comm
package perf

import avro.runtime._
import avro.marker.{AvroRecord, AvroUnion}
import actors.Actor
import com.sun.xml.internal.ws.developer.MemberSubmissionAddressing.Validation

sealed trait PerfMessage extends AvroUnion
case class Ping(var x: Int) extends PerfMessage with AvroRecord
case class Pong(var y: Int) extends PerfMessage with AvroRecord

object ActorPerfTest {
  object PerfRegistry extends ServiceRegistry[PerfMessage]

  class TestActor extends Actor {
    implicit val remoteHandle = PerfRegistry.registerActor(this)
    override def act(): Unit = {
      loop {
        react {
          case Envelope(src, Ping(x)) =>
            src.foreach(_.asInstanceOf[RemoteService[PerfMessage]] ! Pong(x))
            //Shutdown and unregister actor
            exit()
            PerfRegistry.unregisterService(remoteHandle)
        }
      }
    }
  }

  def actorMessage: Unit = {
    val actor = (new TestActor)
    actor.start
    actor.remoteHandle !? Ping(1)
  }

  def handleDispatchMessage(msg: Envelope[PerfMessage]): Unit = {
    msg.src.foreach(_ !! Pong(1))
  }

  def dispatchMessage: Unit = {
    val actor = PerfRegistry.registerActor(handleDispatchMessage _)
    actor !? Ping(1)
  }

  def main(args: Array[String]): Unit = {
    val msgCount = 100000
    (1 to  10).foreach(i => {
      val actStart = System.nanoTime()
      (1 to 100000).foreach(j => {
         actorMessage
      })
      val actEnd = System.nanoTime()
      println("actors: " + (msgCount / ((actEnd - actStart) / 1000000000)))

      val distStart = System.nanoTime()
      (1 to 100000).foreach(j => {
         dispatchMessage
      })
      val distEnd = System.nanoTime()
      println("hawt: " + (msgCount / ((distEnd - distStart)/ 1000000000)))
    })
  }
}