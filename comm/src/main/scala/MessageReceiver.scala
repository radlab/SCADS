package edu.berkeley.cs.scads.comm

import scala.actors._
import scala.concurrent.SyncVar

import java.util.concurrent.{ BlockingQueue, ArrayBlockingQueue, 
                              CountDownLatch, ThreadPoolExecutor, TimeUnit}

import org.apache.log4j.Logger

trait MessageReceiver {
  def receiveMessage(src: Option[RemoteActorProxy], msg:MessageBody): Unit
}

case class ActorService(a: Actor) extends MessageReceiver {
  def receiveMessage(src: Option[RemoteActorProxy], msg: MessageBody): Unit =  {
    src match {
      case Some(ra) => a.send(msg, ra.outputChannel)
      case None => a ! msg
    }
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
abstract class ServiceHandler[MessageType <: MessageBody] extends MessageReceiver {

  protected val logger: Logger

  /* Threadpool for execution of incoming requests */
  protected val outstandingRequests = new ArrayBlockingQueue[Runnable](1024) // TODO: read from config
  protected val executor = new ThreadPoolExecutor(5, 20, 30, TimeUnit.SECONDS, outstandingRequests)

  /* Latch for waiting for startup to finish */
  private val startupGuard = new CountDownLatch(1)

  /* Registration (publication of this instance) must happen after
   * initialization statements above. otherwise, NPE will ensue if a message
   * arrives immediately (since the thread pool has not been initialized yet,
   * etc) */
  implicit val remoteHandle = MessageHandler.registerService(this)

  // TODO: use an explicit startup pattern - see warning message above
  startup()
  startupGuard.countDown() /* signals startup completion */

  /* Guaranteed that no invocations to process will occur during method */
  protected def startup(): Unit
  /* Guaranteed that no invocations to process will occur during method */
  protected def shutdown(): Unit

  /* Callback for when a message is received */
  protected def process(src: Option[RemoteActorProxy], msg: MessageType): Unit

  def stop: Unit = {
    startupGuard.await() /* Let the service start up properly first, before shutting down */
    MessageHandler.unregisterActor(remoteHandle)
    executor.shutdown()
    shutdown()
  }

  /* Request handler class to be executed on this StorageHandlers threadpool */
  class Request(src: Option[RemoteActorProxy], req: MessageBody) extends Runnable {
    def run():Unit = req match {
      case op: MessageType =>
        try { startupGuard.await(); process(src, op) } catch {
          case e: Throwable => {
            /* Get the stack trace */
            val stackTrace = e.getStackTrace().mkString("\n")
            /* Log and report the error */
            logger.error("Exception processing storage request: " + e)
            logger.error(stackTrace)
            src.foreach(_ ! ProcessingException(e.toString, stackTrace))
          }
        }
      case otherMessage: MessageBody => src.foreach(_ ! RequestRejected("Unexpected message type to a storage service.", req))
    }
  }

  /* Enque a recieve message on the threadpool executor */
  def receiveMessage(src: Option[RemoteActorProxy], msg:MessageBody): Unit = {
    try executor.execute(new Request(src, msg)) catch {
      case ree: java.util.concurrent.RejectedExecutionException => src.foreach(_ ! RequestRejected("Thread Pool Full", msg))
      case e: Throwable => {
        /* Get the stack trace */
        var stackTrace = e.getStackTrace().mkString("\n")
        /* Log and report the error */
        logger.error("Exception enquing storage request for execution: " + e)
        logger.error(stackTrace)
        src.foreach(_ ! ProcessingException(e.toString(), stackTrace))
      }
    }
  }
}
