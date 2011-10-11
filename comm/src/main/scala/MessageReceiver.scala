package edu.berkeley.cs.scads.comm

import scala.actors.Actor

import java.util.concurrent.{BlockingQueue, ArrayBlockingQueue, CountDownLatch, ThreadPoolExecutor, TimeUnit}

import net.lag.logging.Logger
import org.apache.avro.generic.IndexedRecord
import org.fusesource.hawtdispatch.Dispatch

trait MessageReceiver[MessageType <: IndexedRecord] {
  def receiveMessage(src: Option[RemoteServiceProxy[MessageType]], msg: MessageType): Unit
}

//TODO: remove extra envelope object creation
case class Envelope[MessageType <: IndexedRecord](src: Option[RemoteService[MessageType]], msg: MessageType)
class ActorReceiver[MessageType <: IndexedRecord](actor: Actor) extends MessageReceiver[MessageType] {
  def receiveMessage(src: Option[RemoteServiceProxy[MessageType]], msg: MessageType): Unit = {
    actor ! Envelope(src.asInstanceOf[Option[RemoteService[MessageType]]], msg)
  }
}

class DispatchReceiver[MessageType <: IndexedRecord](f: (Envelope[MessageType]) => Unit) extends MessageReceiver[MessageType] {
  val queue = Dispatch.createQueue()

  class MessageReceive(msg: Envelope[MessageType]) extends Runnable{
    def run(): Unit = f(msg)
  }

  def receiveMessage(src: Option[RemoteServiceProxy[MessageType]], msg: MessageType): Unit = {
    queue.execute(new MessageReceive(Envelope(src.asInstanceOf[Option[RemoteService[MessageType]]], msg)))
  }
}

/**
 * Executes requests of type MessageType on a threadpool by calling a user
 * supplied process method.
 *
 * WARNING: Currently, invocations to `startup` run in the constructor of
 * ServiceHandler. This means that subclasses will not be properly constructed
 * (its constructor will not have run yet) when `startup` is invoked. To get
 * around this issue, any members of a subclass which need to be referenced in
 * `startup` <b>cannot</b> be vals (or NPE will ensue). Either use lazy vals, or
 * methods.
 *
 * TODO: fix this problem
 */
abstract trait ServiceHandler[MessageType <: IndexedRecord] extends MessageReceiver[MessageType] {
  protected val logger: Logger
  def registry: ServiceRegistry[MessageType]

  /* Threadpool for execution of incoming requests */
  protected val outstandingRequests = new ArrayBlockingQueue[Runnable](1024) // TODO: read from config
  protected val executor = new ThreadPoolExecutor(5, 20, 30, TimeUnit.SECONDS, outstandingRequests)

  /* Latch for waiting for startup to finish */
  private val startupGuard = new CountDownLatch(1)

  /* Registration (publication of this instance) must happen after
   * initialization statements above. otherwise, NPE will ensue if a message
   * arrives immediately (since the thread pool has not been initialized yet,
   * etc) */
  implicit lazy val remoteHandle = registry.registerService(this)

  // TODO: use an explicit startup pattern - see warning message above
  startup()
  startupGuard.countDown()

  /* signals startup completion */

  /* Guaranteed that no invocations to process will occur during method */
  protected def startup(): Unit

  /* Guaranteed that no invocations to process will occur during method */
  protected def shutdown(): Unit

  /* Callback for when a message is received */
  protected def process(src: Option[RemoteServiceProxy[MessageType]], msg: MessageType): Unit

  def stop {
    stopListening
    executor.shutdownNow()
    shutdown()
  }

  /**
   * Un-registers this ServiceHandler from the MessageHandler. After calling
   * stopListening, this ServiceHandler will no longer receive new requests.
   * However, its resources will remain open until stop is called explicitly
   */
  def stopListening {
    startupGuard.await() /* Let the service start up properly first, before shutting down */
    registry.unregisterService(remoteHandle)
  }

  /* Request handler class to be executed on this StorageHandlers threadpool */
  class Request(src: Option[RemoteServiceProxy[MessageType]], msg: MessageType) extends Runnable {
    def run(): Unit = {
      try {
        startupGuard.await(); process(src, msg)
      } catch {
        case e: Throwable => {
          /* Get the stack trace */
          val stackTrace = e.getStackTrace().mkString("\n")
          /* Log and report the error */
          logger.warning(e, "Exception processing storage request")
          //TODO: fix me! src.foreach(_ ! ProcessingException(e.toString, stackTrace))
        }
      }
    }
  }

  /* Enque a recieve message on the threadpool executor */
  final def receiveMessage(src: Option[RemoteServiceProxy[MessageType]], msg: MessageType): Unit = {
    try executor.execute(new Request(src, msg)) catch {
      case ree: java.util.concurrent.RejectedExecutionException => //TODO: Fix me: src.foreach(_ ! RequestRejected("Thread Pool Full", msg))
      case e: Throwable => {
        /* Get the stack trace */
        var stackTrace = e.getStackTrace().mkString("\n")
        /* Log and report the error */
        logger.warning(e, "Exception enquing storage request for execution")
        //TODO Fixme: src.foreach(_ ! ProcessingException(e.toString(), stackTrace))
      }
    }
  }
}
