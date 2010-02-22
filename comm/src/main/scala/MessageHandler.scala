package edu.berkeley.cs.scads.comm

import java.lang.ref.WeakReference
import java.lang.ref.ReferenceQueue
import java.util.concurrent.ConcurrentHashMap

import scala.actors._

import org.apache.log4j.Logger
import org.apache.avro.util.Utf8

trait ServiceHandler {
  def receiveMessage(src: RemoteNode, msg:Message)
}

object MessageHandler extends NioAvroChannelManagerBase[Message, Message] {

  class ActorWeakReference(var actor: Actor, val _queue: ReferenceQueue[Actor], val uniqId:Long) extends WeakReference[Actor](actor, _queue) {
    actor = null
  }

  val logger = Logger.getLogger("scads.actorProxy")

  val actorRegistry = new ConcurrentHashMap[Long, ActorWeakReference]
  val deadActors = new ReferenceQueue[Actor]()
  var curActorId:Long = 0

  val serviceRegistry = new ConcurrentHashMap[String, ServiceHandler]

  val actorCleanupThread = new Thread() {
    override def run(): Unit = {
      while(true) {
        val ref = deadActors.remove()
        logger.debug("Garbage collecting actor registration.")
        val success = actorRegistry.remove(ref.asInstanceOf[ActorWeakReference].uniqId)
        logger.debug("Was able to remove successfully? " + success)
      }
    }
  }

  /* TODO: When curActorId hits MAX_LONG, collect all existing actors
   * and give them ids 0-n, and reset curActorId to (n+1) */

  def registerActor(a: Actor): Long = {
    var id:Long = 0
    actorRegistry.synchronized {
      if (actorRegistry.size >= Math.MAX_LONG)
        throw new IllegalStateException("Too many actors!")
      id = curActorId
      curActorId += 1
      val ref = new ActorWeakReference(a, deadActors, id)
      actorRegistry.put(id, ref)
    }
    id
  }

  def getActor(id: Long): Actor = {
    val targetRef = actorRegistry.get(id)
    if(targetRef == null) 
      return null
    val targetActor = targetRef.get()
    if(targetActor == null) {
      logger.warn("Trying to get an actor that has been gc-ed. "+id)
      return null
    }
    return targetActor
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
