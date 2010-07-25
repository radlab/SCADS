package edu.berkeley.cs.scads.comm

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

import scala.actors._
import scala.concurrent.SyncVar

import org.apache.log4j.Logger
import org.apache.avro.util.Utf8


trait ServiceHandler {
  def receiveMessage(src: Option[RemoteActorProxy], msg:MessageBody): Unit
}

case class ActorService(a: Actor) extends ServiceHandler {
  def receiveMessage(src: Option[RemoteActorProxy], msg: MessageBody): Unit =  {
    src match {
      case Some(ra) => a.send(msg, ra.outputChannel)
      case None => a ! msg
    }
  }
}

class MessageFuture extends Future[MessageBody] with ServiceHandler {
  protected[comm] val remoteActor = MessageHandler.registerService(this)
  protected val message = new SyncVar[MessageBody]

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
  def cancel: Unit = MessageHandler.unregisterActor(remoteActor)

  def receiveMessage(src: Option[RemoteActorProxy], msg: MessageBody): Unit = {
    MessageHandler.unregisterActor(remoteActor)
    message.set(msg)
  }
}

/**
 * The message handler for all scads communication.  It maintains a list of all active services
 * in a given JVM and multiplexes the underlying network connections.  Services should be
 * careful to unregister themselves to avoid memory leaks.
 */
object MessageHandler extends NioAvroChannelManagerBase[Message, Message] {
  protected val logger = Logger.getLogger("scads.messagehandler")
  private val curActorId = new AtomicLong
  private val serviceRegistry = new ConcurrentHashMap[ActorId, ServiceHandler]
  protected val hostname = java.net.InetAddress.getLocalHost.getHostName()

  startListener()

  def registerActor(a: Actor): RemoteActorProxy = {
    val id = curActorId.getAndIncrement
    serviceRegistry.put(ActorNumber(id), ActorService(a))
    RemoteActor(hostname, getLocalPort, ActorNumber(id))
  }

  def unregisterActor(ra: RemoteActorProxy): Unit = serviceRegistry.remove(ra.id)

  def registerService(service: ServiceHandler): RemoteActor = {
    val id = ActorNumber(curActorId.getAndIncrement)
    serviceRegistry.put(id, service)
    RemoteActor(hostname, getLocalPort, id)
  }

  def registerService(id: String, service: ServiceHandler): RemoteActorProxy = {
    if (serviceRegistry.containsKey(id))
      throw new IllegalStateException("Service with that ID already registered")
    serviceRegistry.put(ActorName(id),service)
    RemoteActor(hostname, getLocalPort, ActorName(id))
  }

  def getService(id: String):ServiceHandler  = {
    return serviceRegistry.get(id)
  }

  def receiveMessage(src: RemoteNode, msg: Message): Unit = {
    val service = serviceRegistry.get(msg.dest)

    if(service != null) {
      service.receiveMessage(msg.src.map(RemoteActor(src.hostname, src.port, _)), msg.body)
    }
    else
      logger.warn("Got message for an unknown service: " + msg.dest)
  }
}
