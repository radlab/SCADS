package edu.berkeley.cs
package scads
package comm
package performance

import avro.runtime._
import avro.marker.{AvroRecord, AvroUnion}
import actors.Actor
import com.sun.xml.internal.ws.developer.MemberSubmissionAddressing.Validation
import java.awt.image.PixelInterleavedSampleModel
import java.lang.RuntimeException

sealed trait PerfMessage extends AvroUnion
case class Ping(var x: Int) extends PerfMessage with AvroRecord
case class Pong(var y: Int) extends PerfMessage with AvroRecord

object ActorPerfTest {
  implicit object PerfRegistry extends ServiceRegistry[PerfMessage]

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

  val destFuture = new MessageFuture[PerfMessage](null, null)
  def futureMessage: Unit = {
    val ping = Ping(1)
    val f1 = new MessageFuture[PerfMessage](destFuture.remoteService, ping)
    val f2 = f1.remoteService !! ping
    f1.get(1000).getOrElse(throw new RuntimeException("TIMEOUT"))
    f2.remoteService.!(Pong(1))(destFuture.remoteService)
    f2.get(1000).getOrElse(throw new RuntimeException("TIMEOUT"))
  }

  def main(args: Array[String]): Unit = {
    val msgCount = 1000000
    (1 to  10).foreach(i => {
      val actStart = System.nanoTime()
      (1 to msgCount).foreach(j => {
         actorMessage
      })
      val actEnd = System.nanoTime()
      println("actors: " + 2 * (msgCount / ((actEnd - actStart) / 1000000000.0)))

      val distStart = System.nanoTime()
      (1 to msgCount).foreach(j => {
         dispatchMessage
      })
      val distEnd = System.nanoTime()
      println("hawt: " + 2 * (msgCount / ((distEnd - distStart)/ 1000000000.0)))

      val futureStart = System.nanoTime()
      (1 to msgCount).foreach(j => {
         futureMessage
      })
      val futureEnd = System.nanoTime()
      println("future: " + 2 * (msgCount / ((futureEnd - futureStart)/ 1000000000.0)))
    })
  }
}