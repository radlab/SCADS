package edu.berkeley.cs.scads.comm

import java.util.concurrent.{ ConcurrentHashMap, CopyOnWriteArrayList, Executors, TimeUnit }
import java.util.concurrent.atomic.AtomicLong

import scala.actors._

import net.lag.logging.Logger

import edu.berkeley.cs.scads.config._

/**
 * The message handler for all scads communication.  It maintains a list of all active services
 * in a given JVM and multiplexes the underlying network connections.  Services should be
 * careful to unregister themselves to avoid memory leaks.
 */
object MessageHandler extends AvroChannelManager[Message, Message] {

  private val config = Config.config
  private val logger = Logger()

  private val curActorId      = new AtomicLong
  private val serviceRegistry = new ConcurrentHashMap[ActorId, MessageReceiver]

  private val hostname = java.net.InetAddress.getLocalHost.getCanonicalHostName()

  private val listeners = new CopyOnWriteArrayList[MessageHandlerListener[Message, Message]]

  private lazy val delayExecutor = Executors.newScheduledThreadPool(0) /* Don't keep any core threads alive */

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

  /**
   * Implemented in Java style for efficiency concerns
   */
  @inline private def foldLeftListeners(evt: MessageHandlerEvent[Message, Message]): MessageHandlerResponse = {
    if (listeners.isEmpty) RelayMessage
    else {
      val iter     = listeners.iterator
      var continue = iter.hasNext
      var result: MessageHandlerResponse = RelayMessage
      while (continue) {
        val listener = iter.next()
        result = 
          try {
            val res = listener.handleEvent(evt)
            if (res eq null) {
              logger.error("MessageHandlerListener %s returned null, ignoring".format(listener))
              RelayMessage
            } else res
          } catch {
            case e: Exception =>
              logger.error("Caught exception while executing MessageHandlerListener %s".format(listener), e)
              RelayMessage
          }
        result match {
          case RelayMessage => /* Progress down the chain */
            continue = iter.hasNext
          case DropMessage | DelayMessage(_, _) => /* Stop the chain */
            continue = false
        }
      }
      result
    }
  }

  @inline private def toRunnable(f: => Unit) = new Runnable {
    def run() { f }
  }

  // Delegation overrides

  override def sendMessage(dest: RemoteNode, msg: Message) {
    logger.trace("Sending %s to %s", msg, dest)
    val evt = MessagePending[Message, Message](dest, Left(msg))
    foldLeftListeners(evt) match {
      case RelayMessage => impl.sendMessage(dest, msg)
      case DropMessage  => /* Drop the message */
      case DelayMessage(delay, units) =>
        delayExecutor.schedule(toRunnable(impl.sendMessage(dest, msg)), delay, units)        
    }
  }

  override def sendMessageBulk(dest: RemoteNode, msg: Message) {
    val evt = MessagePending[Message, Message](dest, Left(msg))
    foldLeftListeners(evt) match {
      case RelayMessage => impl.sendMessageBulk(dest, msg)
      case DropMessage  => /* Drop the message */
      case DelayMessage(delay, units) =>
        delayExecutor.schedule(toRunnable(impl.sendMessageBulk(dest, msg)), delay, units)        
    }
  }

  override def flush { impl.flush }

  override def startListener(port: Int) { impl.startListener(port) }

  override def receiveMessage(src: RemoteNode, msg: Message) {
    throw new AssertionError("Should not be called- doReceiveMessage should be called instead")
  }

  private def doReceiveMessage(src: RemoteNode, msg: Message) {
    logger.trace("Received message %s from %s", msg, src)
    val evt = MessagePending[Message, Message](src, Right(msg))
    foldLeftListeners(evt) match {
      case RelayMessage => doReceiveMessage0(src, msg)
      case DropMessage  => /* Drop the message */
      case DelayMessage(delay, units) =>
        delayExecutor.schedule(toRunnable(doReceiveMessage0(src, msg)), delay, units)        
    }
  }

  private def doReceiveMessage0(src: RemoteNode, msg: Message) {
    val service = serviceRegistry.get(msg.dest)

    logger.trace("Received Message: %s from %s", msg, src)

    if(service != null) {
      service.receiveMessage(msg.src.map(RemoteActor(src.hostname, src.port, _)), msg.body)
    }
    else
      logger.critical("Got message for an unknown service: " + msg.dest)
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
          logger.critical("Could not listen on port %d, trying %d".format(port, port + 1))
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

  /**
   * Register a MessageHandlerListener. Listeners are registered in FIFO
   * order. Note that priority is given to the beginning listeners, meaning if
   * listener A comes before B, and A drops/delays a message, B will never receive
   * notification of that message. Only if A relays the message will B get a
   * chance to act on the message
   *
   * Note: This implementation does check if listener is already registered.
   * If so, this is a no-op (priority is also not changed). Also, registering
   * (and unregistering) is a fairly expensive operation, since the backing
   * list is implemented as a COW list, so it is recommended that listeners
   * are added as part of an initialization sequence, and not touched then.
   */ 
  def registerListener(listener: MessageHandlerListener[Message, Message]) {
    listeners.addIfAbsent(listener)
  }

  /**
   * Unregisters a MessageHandlerListener.
   *
   * Note: Is a no-op if listener is not already registered
   */
  def unregisterListener(listener: MessageHandlerListener[Message, Message]) {
    listeners.remove(listener)
  }

}

sealed abstract class MessageHandlerEvent[SendType, RecvType]

/**
 * Indicates that either a message is about to be sent, or a message is about
 * to be received. A LeftProjection of message indicates the former, a
 * RightProjection indicates the latter
 */
case class MessagePending[SendType, RecvType](remote: RemoteNode, msg: Either[SendType, RecvType])
  extends MessageHandlerEvent[SendType, RecvType]

sealed abstract class MessageHandlerResponse

/**
 * Drop the message corresponding to the MessageHandlerEvent entirely
 */
case object DropMessage extends MessageHandlerResponse

/**
 * Delay the message corresponding to the MessageHandlerEvent by delay. If the
 * message is about to be sent, this means don't send it until delay has
 * elasped. If the message is about to be received, this means don't receive
 * the message until the delay has elasped.
 */
case class DelayMessage(delay: Long, units: TimeUnit)
  extends MessageHandlerResponse

/**
 * Resume the regular course of action for the message
 */
case object RelayMessage extends MessageHandlerResponse

/**
 * Base trait for a MessageHandler event listener
 */
trait MessageHandlerListener[SendType, RecvType] {

  /**
   * Main method for listeners to supply.
   */
  def handleEvent(evt: MessageHandlerEvent[SendType, RecvType]): MessageHandlerResponse

}
