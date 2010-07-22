package edu.berkeley.cs.scads.comm

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

import scala.actors._

import org.apache.log4j.Logger
import org.apache.avro.util.Utf8


trait ServiceHandler {
  def receiveMessage(src: Option[RemoteActor], msg:MessageBody): Unit
}

case class ActorService(a: Actor) extends ServiceHandler {
  def receiveMessage(src: Option[RemoteActor], msg: MessageBody): Unit =  {
    src match {
      case Some(ra) => a.send(msg, ra.outputChannel)
      case None => a ! msg
    }
  }
}

object MessageHandler extends NioAvroChannelManagerBase[Message, Message] {
  val logger = Logger.getLogger("scads.MessageHandler")
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

  /* TODO: deprecate in favor of native actor communication */
  def registerActor(a: Actor): RemoteActor = {
    val id = curActorId.getAndIncrement
    serviceRegistry.put(ActorNumber(id), ActorService(a))
    RemoteActor(hostname, port, ActorNumber(id))
  }

  def unregisterActor(ra: RemoteActor): Unit = serviceRegistry.remove(ra.id)

  @deprecated("don't use")
  def getActor(id: Long): Actor = serviceRegistry.get(id) match {
    case ActorService(a) => a
    case _ => throw new RuntimeException("Asked for an actor found a service.  Don't use this method anyway... its been deprecated")
  }

  def registerService(service: ServiceHandler): RemoteActor = {
    val id = ActorNumber(curActorId.getAndIncrement)
    serviceRegistry.put(id, service)
    RemoteActor(hostname, port, id)
  }

  def registerService(id: String, service: ServiceHandler): RemoteActor = {
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
