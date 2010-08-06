package edu.berkeley.cs.scads.comm

import scala.actors._
import scala.concurrent.SyncVar

import java.util.concurrent.{BlockingQueue, ArrayBlockingQueue, ThreadPoolExecutor, TimeUnit}

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
 */
abstract class ServiceHandler[MessageType <: MessageBody] extends MessageReceiver {
  implicit val remoteHandle = MessageHandler.registerService(this)
  protected val logger: Logger

  /* Threadpool for execution of incoming requests */
  protected val outstandingRequests = new ArrayBlockingQueue[Runnable](1024)
  protected val executor = new ThreadPoolExecutor(5, 20, 30, TimeUnit.SECONDS, outstandingRequests)

  startup()

  protected def startup(): Unit
  protected def shutdown(): Unit
  protected def process(src: Option[RemoteActorProxy], msg: MessageType): Unit

  def stop: Unit = {
    MessageHandler.unregisterActor(remoteHandle)
    executor.shutdown()
    shutdown()
  }

  /* Request handler class to be executed on this StorageHandlers threadpool */
  class Request(src: Option[RemoteActorProxy], req: MessageBody) extends Runnable {
    def run():Unit = req match {
      case op: MessageType =>
        try process(src, op) catch {
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
