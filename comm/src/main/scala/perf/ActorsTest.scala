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
import javax.management.remote.rmi._RMIConnection_Stub
import java.util.concurrent.{CountDownLatch, PriorityBlockingQueue}
import performance.ActorPerfTest.{BatchSender, BatchReceiver}

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

  class BatchReceiver(){
    var counter = 0

    implicit val actor = PerfRegistry.registerBatchActor(process)

    def process(mailbox : PriorityBlockingQueue[PrioEnvelope[PerfMessage]], interrupts : Int) : Unit = {
      val iter = mailbox.iterator()
      while(iter.hasNext){
        val msg = iter.next()
        msg.src.get.!(Pong(1))(actor)
        counter += 1
        iter.remove()
      }
      println("BatchReceiver size" + mailbox.size() + " total:" + counter + " i:" + interrupts)
    }

    def unregister = PerfRegistry.unregisterService(actor)
  }

  class BatchSender(){

    val barrier = new CountDownLatch(1)

    @volatile var counter : Int= 0
    def send(target : RemoteServiceProxy[PerfMessage], msgCount : Int) = {
      counter = msgCount
      (1 to msgCount).foreach(j => {
        target.!(Ping(1))(actor)
      })

    }

    implicit val actor = PerfRegistry.registerBatchActor(process)

    def process(mailbox : PriorityBlockingQueue[PrioEnvelope[PerfMessage]], interrupts : Int) : Unit = {
      val iter = mailbox.iterator()
      while(iter.hasNext){
        val msg = iter.next()
        counter -= 1
        iter.remove()
        if(counter == 0){
          barrier.countDown()
        }
      }
      println("BatchSender End size" + mailbox.size() + " counter" + counter + " i:" + interrupts)
    }

    def unregister = PerfRegistry.unregisterService(actor)
  }

  def dispatchMessage: Unit = {
    val actor = PerfRegistry.registerActorFunc {
      case Envelope(src, Ping(x)) => src.foreach(_ !! Pong(x))
    }
    actor !? Ping(1)
    PerfRegistry.unregisterService(actor)
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
//      val actStart = System.nanoTime()
//      (1 to msgCount).foreach(j => {
//         actorMessage
//      })
//      val actEnd = System.nanoTime()
//      println("actors: " + 2 * (msgCount / ((actEnd - actStart) / 1000000000.0)))
//
//      val distStart = System.nanoTime()
//      (1 to msgCount).foreach(j => {
//         dispatchMessage
//      })
//      val distEnd = System.nanoTime()
//      println("hawt: " + 2 * (msgCount / ((distEnd - distStart)/ 1000000000.0)))
//
//      val futureStart = System.nanoTime()
//      (1 to msgCount).foreach(j => {
//         futureMessage
//      })
//      val futureEnd = System.nanoTime()
//      println("future: " + 2 * (msgCount / ((futureEnd - futureStart)/ 1000000000.0)))

      val batchStart = System.nanoTime()
      val batchReceiver = new BatchReceiver
      val batchSender = new BatchSender
      batchSender.send(batchReceiver.actor, msgCount)
      batchSender.barrier.await()
      batchSender.unregister
      batchReceiver.unregister
      val batchEnd = System.nanoTime()
      println("Batch: " + 2 * (msgCount / ((batchEnd - batchStart)/ 1000000000.0)))
    })
  }
}