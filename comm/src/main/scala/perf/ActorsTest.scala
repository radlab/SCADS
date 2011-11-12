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
import performance.ActorPerfTest.{BatchSender, BatchReceiver}
import _root_.org.fusesource.hawtdispatch._
import java.util.concurrent.{ArrayBlockingQueue, CountDownLatch, PriorityBlockingQueue}

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

  class BatchReceiver(fast : Boolean){
    var counter = 0

    implicit val actor = if(fast)
        PerfRegistry.registerFastMailboxFunc(process)
      else
        PerfRegistry.registerMailboxFunc(_ => 0, process)

    def process(mailbox : Mailbox[PerfMessage]) : Unit = {
      //println("BatchReceiver Start size" + mailbox.size() + " total:" + counter )
      while(mailbox.hasNext){
        val msg = mailbox.next()
        msg.src.get.!(Pong(1))(actor)
        counter += 1
        mailbox.remove()
      }
      //println("BatchReceiver End size" + mailbox.size() + " total:" + counter )
    }

    def unregister = {
      PerfRegistry.unregisterService(actor)

    }
  }

  class BatchSender(fast : Boolean){

    val barrier = new CountDownLatch(1)

    @volatile var counter : Int= 0
    def send(target : RemoteServiceProxy[PerfMessage], msgCount : Int) = {
      counter = msgCount
      (1 to msgCount).foreach(j => target.!(Ping(1))(actor))

//      (1 to msgCount).foreach(j => {
//        globalQueue {
//          (1 to 100).foreach(i => target.!(Ping(1))(actor))
//        }
//      })

    }

    implicit val actor = if(fast)
        PerfRegistry.registerFastMailboxFunc(process)
      else
        PerfRegistry.registerMailboxFunc(_ => 0, process)

    def process(mailbox : Mailbox[PerfMessage]) : Unit = {
      //println("BatchSender Start size" + mailbox.size() + " counter" + counter )
      while(mailbox.hasNext){
        val msg = mailbox.next()
        counter -= 1
        mailbox.remove()
        if(counter == 0){
          barrier.countDown()
          //println("Releasing barrier")
        }
      }
      //println("BatchSender End size" + mailbox.size() + " counter" + counter )
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
    val msgCount = 100000 //1000000
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

      val batchStart = System.nanoTime()
      val batchReceiver = new BatchReceiver(false)
      val batchSender = new BatchSender(false)
      batchSender.send(batchReceiver.actor, msgCount)
      batchSender.barrier.await()
      batchSender.unregister
      batchReceiver.unregister
      val batchEnd = System.nanoTime()
      println("Mailbox: " + 2 * (msgCount / ((batchEnd - batchStart)/ 1000000000.0)))

      val mbSourceStart = System.nanoTime()
      val mbSourceReceiver = new BatchReceiver(true)
      val mbSourceSender = new BatchSender(true)
      mbSourceSender.send(mbSourceReceiver.actor, msgCount)
      mbSourceSender.barrier.await()
      mbSourceSender.unregister
      mbSourceReceiver.unregister
      val mbSourceEnd = System.nanoTime()
      println("Mailbox Fast: " + 2 * (msgCount / ((mbSourceEnd - mbSourceStart)/ 1000000000.0)))
    })
  }
}