package edu.berkeley.cs.scads.comm

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

import scala.actors._

import org.apache.log4j.Logger

/**
 * The message handler for all scads communication.  It maintains a list of all active services
 * in a given JVM and multiplexes the underlying network connections.  Services should be
 * careful to unregister themselves to avoid memory leaks.
 */
object MessageHandler extends NioAvroChannelManagerBase[Message, Message] {
  protected val logger = Logger.getLogger("scads.messagehandler")
  private val curActorId = new AtomicLong
  private val serviceRegistry = new ConcurrentHashMap[ActorId, MessageReceiver]
  protected val hostname = java.net.InetAddress.getLocalHost.getHostName()

  startListener()

  def registerActor(a: Actor): RemoteActorProxy = {
    val id = curActorId.getAndIncrement
    serviceRegistry.put(ActorNumber(id), ActorService(a))
    RemoteActor(hostname, getLocalPort, ActorNumber(id))
  }

  def unregisterActor(ra: RemoteActorProxy): Unit = serviceRegistry.remove(ra.id)

  def registerService(service: MessageReceiver): RemoteActor = {
    val id = ActorNumber(curActorId.getAndIncrement)
    serviceRegistry.put(id, service)
    RemoteActor(hostname, getLocalPort, id)
  }

  def registerService(id: String, service: MessageReceiver): RemoteActorProxy = {
    if (serviceRegistry.containsKey(id))
      throw new IllegalStateException("Service with that ID already registered")
    serviceRegistry.put(ActorName(id),service)
    RemoteActor(hostname, getLocalPort, ActorName(id))
  }

  def getService(id: String):MessageReceiver  = {
    return serviceRegistry.get(id)
  }

  def receiveMessage(src: RemoteNode, msg: Message): Unit = {
    val service = serviceRegistry.get(msg.dest)

    //Ligher weight log4j that doesn't do string concat unless needed
    edu.berkeley.Log2.debug(logger, "Received Message: ", src, " ", msg)

    if(service != null) {
      service.receiveMessage(msg.src.map(RemoteActor(src.hostname, src.port, _)), msg.body)
    }
    else
      logger.warn("Got message for an unknown service: " + msg.dest)
  }
}
