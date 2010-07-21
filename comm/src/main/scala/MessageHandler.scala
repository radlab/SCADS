package edu.berkeley.cs.scads.comm

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

import scala.actors._

import org.apache.log4j.Logger
import org.apache.avro.util.Utf8


trait ServiceHandler {
  def receiveMessage(src: RemoteNode, msg:Message): Unit
}

case class ActorService(a: Actor) extends ServiceHandler {
  def receiveMessage(src: RemoteNode, msg: Message): Unit =  {
    a ! (src, msg)
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
  def registerActor(a: Actor): Long = {
    val id = curActorId.getAndIncrement
    serviceRegistry.put(ActorNumber(id), ActorService(a))
    id
  }

  def unregisterActor(id:Long): Unit = serviceRegistry.remove(ActorNumber(id))

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
    if(service != null)
      service.receiveMessage(src, msg)
    else
      logger.warn("Got message for an unknown service: " + msg.dest)
  }
}

class StorageEchoServer extends NioAvroChannelManagerBase[Message, Message] {
  def receiveMessage(src: RemoteNode, req: Message): Unit = {
    val resp = new Message(ActorName("echo"), req.src, None, new Record("test".getBytes, "test".getBytes))

    sendMessage(src, resp)
  }
}

class StorageEchoPrintServer extends StorageEchoServer {
  val lock = new Object
  var numMsgs = 0
  override def receiveMessage(src: RemoteNode, req: Message): Unit = {
    lock.synchronized {
      numMsgs += 1
      if (numMsgs % 100000 == 0) println("On msg: " + numMsgs)
    }
    super.receiveMessage(src, req)
  }
}

class StorageDiscardServer extends NioAvroChannelManagerBase[Message, Message] {
  def receiveMessage(src: RemoteNode, req: Message): Unit = { }
}
