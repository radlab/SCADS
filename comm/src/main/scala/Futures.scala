package edu.berkeley.cs.scads.comm

import scala.actors._
import scala.concurrent.SyncVar
import org.apache.log4j.Logger

import java.util.Queue
import java.util.concurrent.ConcurrentLinkedQueue

object MessageFuture {
  implicit def toFutureCollection(futures: Seq[MessageFuture]): FutureCollection = new FutureCollection(futures)
}

class MessageFuture extends Future[MessageBody] with MessageReceiver {
  protected[comm] val remoteActor = MessageHandler.registerService(this)
  protected val sender = new SyncVar[Option[RemoteActorProxy]]
  protected val message = new SyncVar[MessageBody]
  protected var forwardList: List[Queue[MessageFuture]] = List() 

  /* Note: doesn't really implement interface correctly */
  def inputChannel = new InputChannel[MessageBody] {
    def ?(): MessageBody = message.get
    def reactWithin(msec: Long)(pf: PartialFunction[Any, Unit]): Nothing = throw new RuntimeException("Unimplemented")
    def react(f: PartialFunction[MessageBody, Unit]): Nothing = throw new RuntimeException("Unimplemented")
    def receive[R](f: PartialFunction[MessageBody, R]): R = f(message.get)
    def receiveWithin[R](msec: Long)(f: PartialFunction[Any, R]): R = f(message.get(msec).getOrElse(ProcessingException("timeout", "")))
  }

  def respond(r: (MessageBody) => Unit): Unit = r(message.get)
  def apply(): MessageBody = message.get
  def get(timeout: Int): Option[MessageBody] = message.get(timeout)
  def isSet: Boolean = message.isSet

  /**
   * Cancel this request by unregistering with the message handler.
   * Note no action is taken to cancel processing initiating message server-side.
   */
  def cancel: Unit = MessageHandler.unregisterActor(remoteActor)

  /**
   * Either forward the result to the following queue, or request that it be forwarded upon arrival.
   */
  def forward(dest: Queue[MessageFuture]): Unit = synchronized {
    if(message.isSet)
      dest.offer(this)
    else
      forwardList ::= dest
  }

  def receiveMessage(src: Option[RemoteActorProxy], msg: MessageBody): Unit = synchronized {
    MessageHandler.unregisterActor(remoteActor)
    message.set(msg)
    sender.set(src)
    forwardList.foreach(_.offer(this))
  }
}

class FutureCollection(val futures: Seq[MessageFuture]) {
  val responses = new java.util.concurrent.ConcurrentLinkedQueue[MessageFuture]
  futures.foreach(_.forward(responses))

  def blockFor(count: Int): Seq[MessageFuture] = (1 to count).map(_ => responses.poll())
}

