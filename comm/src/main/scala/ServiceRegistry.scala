package edu.berkeley.cs.scads.comm

import java.util.concurrent.{ConcurrentHashMap, CopyOnWriteArrayList, Executors, TimeUnit}
import scala.actors._

import net.lag.logging.Logger

import edu.berkeley.cs.scads.config._

import org.apache.commons.httpclient._
import org.apache.commons.httpclient.methods._
import org.apache.commons.httpclient.params._
import java.util.concurrent.atomic.AtomicLong
import org.apache.avro.generic.{GenericData, IndexedRecord}
import org.apache.avro.Schema
import edu.berkeley.cs.avro.marker.{AvroRecord, AvroUnion}

import remote.RemoteActor
import scala.collection.JavaConversions._
import edu.berkeley.cs.avro.runtime._
import org.apache.avro.Schema.Field
import org.apache.avro.specific.SpecificRecord

/* General message types */
sealed trait ServiceId extends AvroUnion

case class ServiceNumber(var num: Long) extends AvroRecord with ServiceId

case class ServiceName(var name: String) extends AvroRecord with ServiceId

/**
 * The message handler for avro message passing.  It maintains a list of all active services
 * in a given JVM and multiplexes the underlying network connections.  Services should be
 * careful to unregister themselves to avoid memory leaks.
 */
class ServiceRegistry[MessageType <: IndexedRecord](implicit schema: TypedSchema[MessageType]) {

  private val config = Config.config
  val logger = Logger()

  private val curActorId = new AtomicLong
  private val serviceRegistry = new ConcurrentHashMap[ServiceId, MessageReceiver[MessageType]]

  def registrySize = serviceRegistry.size()

  def futureCount = curActorId.get()

  private val hostname = 
    if(System.getProperty("scads.comm.externalip") == null) {
      logger.debug("Using ip address from java.net.InetAddress.getLocalHost")
      java.net.InetAddress.getLocalHost.getCanonicalHostName()
    }
    else {
      val httpClient = new HttpClient()
      val getMethod = new GetMethod("http://instance-data/latest/meta-data/public-hostname")
      httpClient.executeMethod(getMethod)
      val externalIP = getMethod.getResponseBodyAsString
      logger.info("Using external ip address on EC2: %s", externalIP)
      externalIP
    }

  private val listeners = new CopyOnWriteArrayList[MessageHandlerListener]

  private lazy val delayExecutor = Executors.newScheduledThreadPool(0) /* Don't keep any core threads alive */

  private val recvMsgCallback = (_: AvroChannelManager[MessageEnvelope, MessageEnvelope], src: RemoteNode, msg: MessageEnvelope) => {
    doReceiveMessage(src, msg)
  }

  /**
   * Manually created schema for
   * case class MessageEnvelope[MessageType](var src: Option[ActorId], var dest: ActorId, var id: Option[Long], var body: MessageType) extends AvroRecord
   */
  protected val envelopeSchema = new TypedSchema[MessageEnvelope](Schema.createRecord(
    new Schema.Field("src", schemaOf[ServiceId], null, null) ::
    new Schema.Field("dest", schemaOf[ServiceId], null, null) ::
    new Schema.Field("msg", schema, null, null) :: Nil),
    this.getClass.getClassLoader)

  private lazy val impl =
    try getImpl
    catch {
      case e: Exception =>
        logger.error(e, "Could not initialize channel manager implementation")
        throw e
    }

  private def getImpl = {
    val clzName = config.getString(
      "scads.comm.handlerClass",
      classOf[netty.DefaultNettyChannelManager[_, _]].getName)
    logger.info("Using handler impl class: %s".format(clzName))
    logger.debug("ServiceRegistry using classloader %s", schema.classLoader)

    val clz = Class.forName(clzName).asInstanceOf[Class[AvroChannelManager[MessageEnvelope, MessageEnvelope]]]
    val ctor = clz.getConstructor(classOf[Function3[_, _, _, _]], classOf[ClassLoader], classOf[TypedSchema[_]], classOf[TypedSchema[_]])
    ctor.newInstance(recvMsgCallback, this.getClass.getClassLoader, envelopeSchema, envelopeSchema)
  }

  /**
   * Implemented in Java style for efficiency concerns
   */
  @inline private def foldLeftListeners(evt: MessageHandlerEvent): MessageHandlerResponse = {
    if (listeners.isEmpty) RelayMessage
    else {
      val iter = listeners.iterator
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
    def run() {
      f
    }
  }

  def sendMessage(src: Option[RemoteServiceProxy[MessageType]], dest: RemoteServiceProxy[MessageType], msg: MessageType) {
    logger.trace("Sending %s to %s", msg, dest)

    val packaged = new GenericData.Record(envelopeSchema)
    src.foreach(s => packaged.put(0, s.id))
    packaged.put(1, dest.id)
    packaged.put(2, msg)

    val evt = MessagePending(dest, Left(packaged))
    foldLeftListeners(evt) match {
      case RelayMessage => impl.sendMessage(dest.remoteNode, packaged)
      case DropMessage => /* Drop the message */
      case DelayMessage(delay, units) =>
        delayExecutor.schedule(toRunnable(impl.sendMessage(dest.remoteNode, packaged)), delay, units)
    }
  }

  def startListener(port: Int) {
    impl.startListener(port)
  }

  @deprecated("shouldn't be called", "v2.1.2")
  def receiveMessage(src: RemoteNode, msg: MessageType) {
    throw new AssertionError("Should not be called- doReceiveMessage should be called instead")
  }

  private def doReceiveMessage(src: RemoteNode, msg: MessageEnvelope) {
    logger.trace("Received message: %s from %s", msg, src)
    val evt = MessagePending(src, Right(msg))
    foldLeftListeners(evt) match {
      case RelayMessage => doReceiveMessage0(src, msg)
      case DropMessage => /* Drop the message */
      case DelayMessage(delay, units) =>
        delayExecutor.schedule(toRunnable(doReceiveMessage0(src, msg)), delay, units)
    }
  }

  val invalidMessageCount = new java.util.concurrent.atomic.AtomicLong

  private def doReceiveMessage0(src: RemoteNode, msg: MessageEnvelope) {
    val dest = msg.get(1).asInstanceOf[ServiceId]
    val service = serviceRegistry.get(dest)

    logger.trace("Delivering Message: %s from %s", msg, src)

    if (service != null) {
      val srcProxy = if(msg.get(0) == null) None else Some(RemoteService[MessageType](src.hostname, src.port, msg.get(0).asInstanceOf[ServiceId]))
      srcProxy.foreach(_.registry = this)
      service.receiveMessage(srcProxy, msg.get(2).asInstanceOf[MessageType])
    }
    else {
      logger.debug("Got message for an unknown service: %s %s %s", src, dest, msg)
      invalidMessageCount.incrementAndGet()
    }
  }

  /**Immediately start listener on instantiation */
  private val localPort = initListener()

  /**Naively increments port until a valid one is found */
  private def initListener() = {
    var port = config.getInt("scads.comm.listen", 9000)
    var numTries = 0
    var found = false
    while (!found && numTries < 500) {
      try {
        startListener(port)
        found = true
      } catch {
        case ex: org.jboss.netty.channel.ChannelException =>
          logger.debug("Could not listen on port %d, trying %d".format(port, port + 1))
          port += 1
      } finally {
        numTries += 1
      }
    }
    if (found)
      port
    else throw new RuntimeException("Could not initialize listening port in 50 tries")
  }

  def unregisterService(service: RemoteServiceProxy[MessageType]) = {
    serviceRegistry.remove(service.id)
  }

  def registerService(service: MessageReceiver[MessageType]): RemoteServiceProxy[MessageType] = {
    val id = ServiceNumber(curActorId.getAndIncrement)
    serviceRegistry.put(id, service)
    val svc = RemoteService[MessageType](hostname, localPort, id)
    svc.registry = this
    svc
  }

  def registerService(id: String, service: MessageReceiver[MessageType]): RemoteServiceProxy[MessageType] = {
    val key = ServiceName(id)
    val value0 = serviceRegistry.putIfAbsent(key, service)
    if (value0 ne null)
      throw new IllegalArgumentException("Service with %s already registered: %s".format(id, service))
    val svc = RemoteService[MessageType](hostname, localPort, key)
    svc.registry = this
    svc
  }

  /**
   * Register an actor so that it can receive external messages
   */
  def registerActor(actor: actors.Actor): RemoteServiceProxy[MessageType] = {
    val receiver = new ActorReceiver[MessageType](actor)
    registerService(receiver)
  }

  def getService(id: String): MessageReceiver[MessageType] =
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
  def registerListener(listener: MessageHandlerListener) {
    listeners.addIfAbsent(listener)
  }

  /**
   * Unregisters a MessageHandlerListener.
   *
   * Note: Is a no-op if listener is not already registered
   */
  def unregisterListener(listener: MessageHandlerListener) {
    listeners.remove(listener)
  }

}

sealed abstract class MessageHandlerEvent

/**
 * Indicates that either a message is about to be sent, or a message is about
 * to be received. A LeftProjection of message indicates the former, a
 * RightProjection indicates the latter
 */
case class MessagePending(remote: Any, msg: Either[MessageEnvelope, MessageEnvelope])
  extends MessageHandlerEvent

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
trait MessageHandlerListener {

  /**
   * Main method for listeners to supply.
   */
  def handleEvent(evt: MessageHandlerEvent): MessageHandlerResponse
}
