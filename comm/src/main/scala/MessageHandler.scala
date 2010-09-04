package edu.berkeley.cs.scads.comm

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

import scala.actors._

import org.apache.log4j.Logger

import edu.berkeley.cs.scads.config._

/**
 * The message handler for all scads communication.  It maintains a list of all active services
 * in a given JVM and multiplexes the underlying network connections.  Services should be
 * careful to unregister themselves to avoid memory leaks.
 */
class MessageHandler extends AvroChannelManager[Message, Message] {

  private val config = Config.config
  private val logger = Logger.getLogger("scads.messagehandler")

  private val curActorId      = new AtomicLong
  private val serviceRegistry = new ConcurrentHashMap[ActorId, MessageReceiver]

  private val hostname = java.net.InetAddress.getLocalHost.getHostName()

  private lazy val impl = 
    try getImpl
    catch {
      case e: Exception =>
        logger.error("Could not initialize handler implementation, using default impl", e)
        val recvMsgCallback = (_: AvroChannelManager[Message, Message], src: RemoteNode, msg: Message) => {
          doReceiveMessage(src, msg)
        }
        new DefaultNioChannelManager[Message, Message](recvMsgCallback, classOf[Message], classOf[Message])
    }

  private def getImpl = {
    val clzName = 
      config.getString("scads.comm.handlerClass", classOf[DefaultNioChannelManager[_,_]].getName)
    logger.info("Using handler impl class: %s".format(clzName))

    val recvMsgCallback = (_: AvroChannelManager[Message, Message], src: RemoteNode, msg: Message) => {
      doReceiveMessage(src, msg)
    }

    // TODO: custom class loader
    val clz  = Class.forName(clzName).asInstanceOf[Class[AvroChannelManager[Message, Message]]]
    val ctor = clz.getConstructor(classOf[Function3[_, _, _, _]], classOf[Class[_]], classOf[Class[_]])
    ctor.newInstance(recvMsgCallback, classOf[Message], classOf[Message])
  }

  // Delegation overrides

  override def sendMessage(dest: RemoteNode, msg: Message) {
    impl.sendMessage(dest, msg)
  }

  override def sendMessageBulk(dest: RemoteNode, msg: Message) {
    impl.sendMessageBulk(dest, msg)
  }

  override def flush { impl.flush }

  override def startListener(port: Int) { impl.startListener(port) }

  override def receiveMessage(src: RemoteNode, msg: Message) {
    impl.receiveMessage(src, msg)
  }

  private def doReceiveMessage(src: RemoteNode, msg: Message) {
    val service = serviceRegistry.get(msg.dest)

    //Ligher weight log4j that doesn't do string concat unless needed
    edu.berkeley.Log2.debug(logger, "Received Message: ", src, " ", msg)

    if(service != null) {
      service.receiveMessage(msg.src.map(RemoteActor(src.hostname, src.port, _)), msg.body)
    }
    else
      logger.warn("Got message for an unknown service: " + msg.dest)
  }

  /** Immediately start listener on instantiation */ 
  private val localPort = initListener() 

  /** Naively increments port until a valid one is found */
  private def initListener() = {
    var port = config.getInt("scads.comm.listen", 9000)
    var numTries = 0
    var found    = false
    while (!found && numTries < 50) {
      try {
        startListener(port)
        found = true
      } catch {
        case ex: Exception => 
          logger.error("Could not listen on port %d".format(port), ex)
          port += 1
      } finally { numTries += 1 }
    }
    if (found)
      port
    else throw new RuntimeException("Could not initialize listening port in 50 tries")
  }

  // Methods for registering/unregistering proxies

  def registerActor(a: Actor): RemoteActorProxy = {
    val id = curActorId.getAndIncrement
    serviceRegistry.put(ActorNumber(id), ActorService(a))
    RemoteActor(hostname, localPort, ActorNumber(id))
  }

  def unregisterActor(ra: RemoteActorProxy) { 
    serviceRegistry.remove(ra.id)
  }

  def registerService(service: MessageReceiver): RemoteActor = {
    val id = ActorNumber(curActorId.getAndIncrement)
    serviceRegistry.put(id, service)
    RemoteActor(hostname, localPort, id)
  }

  def registerService(id: String, service: MessageReceiver): RemoteActorProxy = {
    val key = ActorName(id)
    val value0 = serviceRegistry.putIfAbsent(key, service)
    if (value0 ne null)
      throw new IllegalArgumentException("Service with %s already registered: %s".format(id, service))
    RemoteActor(hostname, localPort, key)
  }

  def getService(id: String): MessageReceiver = 
    serviceRegistry.get(id)

}


object MessageHandler  {

  var instance : MessageHandler = MessageHandlerFactory.create

  def get() : MessageHandler = instance

  def sendMessage(dest: RemoteNode, msg: Message) = instance.sendMessage(dest, msg)

  def sendMessageBulk(dest: RemoteNode, msg: Message) = instance.sendMessageBulk(dest, msg)

  def flush = instance.flush

  def startListener(port: Int) = instance.startListener(port)

  def receiveMessage(src: RemoteNode, msg: Message)  = instance.receiveMessage(src, msg)

  def registerActor(a: Actor): RemoteActorProxy = instance.registerActor(a)

  def unregisterActor(ra: RemoteActorProxy) = instance.unregisterActor(ra)

  def registerService(service: MessageReceiver): RemoteActor = instance.registerService(service)

  def registerService(id: String, service: MessageReceiver): RemoteActorProxy = instance.registerService(id, service)

  def getService(id: String): MessageReceiver = instance.getService(id)
}

object MessageHandlerFactory  {

  var creatorFn : () => MessageHandler = () => {
    println("Created a msgHandler")
   new MessageHandler()
 }

  def create() : MessageHandler = {
     creatorFn()
  }
}