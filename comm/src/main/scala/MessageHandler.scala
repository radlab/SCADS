package edu.berkeley.cs.scads.comm

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

import scala.actors._

import org.apache.log4j.Logger
import org.apache.avro.util.Utf8

trait ServiceHandler {
  def receiveMessage(src: RemoteNode, msg:Message)
}

object MessageHandler extends NioAvroChannelManagerBase[Message, Message] {

  val logger = Logger.getLogger("scads.MessageHandler")

  private val actorRegistry = new ConcurrentHashMap[Long, Actor]
  private val curActorId = new AtomicLong
  private val serviceRegistry = new ConcurrentHashMap[String, ServiceHandler]

  /* TODO: When curActorId hits MAX_LONG, collect all existing actors
   * and give them ids 0-n, and reset curActorId to (n+1) */
  def registerActor(a: Actor): Long = {
    if (actorRegistry.size >= Math.MAX_LONG)
      throw new IllegalStateException("Too many actors!")
    val id = curActorId.getAndIncrement
    actorRegistry.put(id, a)
    id
  }
  
  def unregisterActor(id:Long): Unit = {
    actorRegistry.remove(id)
  }

  def getActor(id: Long): Actor = {
    actorRegistry.get(id)
  }

  def registerService(id: String, service:ServiceHandler): Unit = {
    if (serviceRegistry.containsKey(id))
      throw new IllegalStateException("Service with that ID already registered")
    serviceRegistry.put(id,service)
  }

  def getService(id: String):ServiceHandler  = {
    return serviceRegistry.get(id)
  }

  def receiveMessage(src: RemoteNode, msg: Message): Unit = msg.dest match {
    case l:java.lang.Long => {
      val act = getActor(l.longValue)
      if (act != null) {
        act ! (src,msg)
        return
      }
    }
    case s:String => {
      var service = getService(s)
      if (service != null)
        service.receiveMessage(src,msg)
    }
    case u:Utf8 => {
      var service = getService(u.toString)
      if (service != null)
        service.receiveMessage(src,msg)
    }
    case _ => {
      throw new IllegalStateException("Invalid destination type in message")
    }
  }

}

class StorageEchoServer extends NioAvroChannelManagerBase[Message, Message] {
  def receiveMessage(src: RemoteNode, req: Message): Unit = {
    val resp = new Message
    resp.dest = req.src
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
