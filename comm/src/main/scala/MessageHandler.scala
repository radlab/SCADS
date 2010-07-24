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

class FutureService extends ServiceHandler {
  val remoteActor = MessageHandler.registerService(this)
  val messageFuture = new SyncVar[MessageBody]

  def apply() = messageFuture.get
  def isSet = messageFuture.isSet

  def receiveMessage(src: Option[RemoteActorProxy], msg: MessageBody): Unit = {
    MessageHandler.unregisterActor(remoteActor)
    messageFuture.set(msg)
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
  protected val port = startListener()

  protected def startListener(): Int = {
    var port = 9000
    var open = false
    while(!open) {
      try {
        startListener(port)
        open = true
      }
      catch {
        case e: java.net.BindException => {
          logger.info("Error opening port: " + port + "for message handler.  Trying another port")
          port += 1
        }
      }
    }
    return port
  }

  def registerActor(a: Actor): RemoteActorProxy = {
    val id = curActorId.getAndIncrement
    serviceRegistry.put(ActorNumber(id), ActorService(a))
    RemoteActor(hostname, port, ActorNumber(id))
  }

  def unregisterActor(ra: RemoteActorProxy): Unit = serviceRegistry.remove(ra.id)

  def registerService(service: ServiceHandler): RemoteActor = {
    val id = ActorNumber(curActorId.getAndIncrement)
    serviceRegistry.put(id, service)
    RemoteActor(hostname, port, id)
  }

  def registerService(id: String, service: ServiceHandler): RemoteActorProxy = {
    if (serviceRegistry.containsKey(id))
      throw new IllegalStateException("Service with that ID already registered")
    serviceRegistry.put(ActorName(id),service)
    RemoteActor(hostname, port, ActorName(id))
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
