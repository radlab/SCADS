package edu.berkeley.cs.scads.comm

import net.lag.logging.Logger

import org.apache.avro.generic.IndexedRecord
import edu.berkeley.cs.avro.marker.{AvroUnion, AvroRecord}
import java.util.concurrent.{ArrayBlockingQueue, TimeUnit, ThreadPoolExecutor}

/* Generic Remote Actor Handle */
case class RemoteService[MessageType <: IndexedRecord](var remoteNode: RemoteNode,
                       var id: ServiceId) extends RemoteServiceProxy[MessageType]

case class TimeoutException(msg: IndexedRecord) extends Exception

object RemoteServiceProxy {
  val logger = Logger()
}

trait RemoteServiceProxy[MessageType <: IndexedRecord] {
  var remoteNode: RemoteNode
  var id: ServiceId

  def host = remoteNode.hostname
  def port = remoteNode.port

  import RemoteServiceProxy._

  implicit var registry: ServiceRegistry[MessageType] = null

  override def toString(): String = id + "@" + host + ":" + port



  /**
   * Returns an ouput proxy that forwards any messages to the remote actor with and empty sender.
   */
  /*def outputChannel = new OutputChannel[Any] {
    def !(msg: Any):Unit = msg match {
      case msgBody: MessageType => MessageHandler.sendMessage(remoteNode, Message(None, id, None, msgBody))
      case otherMessage => throw new RuntimeException("Invalid remote message type:" + otherMessage + " Must extend MessageBody.")
    }
    def forward(msg: Any):Unit = throw new RuntimeException("Unimplemented")
    def send(msg: Any, sender: OutputChannel[Any]):Unit = throw new RuntimeException("Unimplemented")
    def receiver: Actor = throw new RuntimeException("Unimplemented")
  }*/

  /**
   * Send a message asynchronously.
   */
  def !(msg: MessageType)(implicit sender: RemoteServiceProxy[MessageType]): Unit = {
    registry.sendMessage(Some(sender), this, msg)
  }

  def forward(msg: MessageType,  sender: RemoteServiceProxy[MessageType]): Unit = {
    registry.sendMessage(Some(sender), this, msg, true)
  }

  /**
   * Send a message and synchronously wait for a response.
   */
  def !?(msg: MessageType, timeout: Int = 10 * 1000): MessageType = {
    val future = new MessageFuture[MessageType](this, msg)
    this.!(msg)(future.remoteService)
    logger.debug("Wating for SyncSend %s", future.remoteService)
    future.get(timeout) match {
      //TODO: fix exception throwing: case Some(exp: RemoteException) => throw new RuntimeException(exp.toString)
      case Some(msg) =>
        logger.debug("SyncSend %s complete ", future.remoteService)
        msg.asInstanceOf[MessageType]
      case None => {
        logger.debug("SyncSend %s timeout", future.remoteService)
        throw TimeoutException(msg)
      }
    }
  }

  /**
   * Sends a message and returns a future for the response.
   */
  def !!(body: MessageType): MessageFuture[MessageType] = {
    val future = new MessageFuture[MessageType](this, body)
    this.!(body)(future.remoteService)
    future
  }

  private var msgQueue = new ArrayBlockingQueue[Runnable](1024)
  protected val sendExecutor = new ThreadPoolExecutor(10, 100, 30, TimeUnit.SECONDS, msgQueue)

  class SendRequest(msg: MessageType, sender: RemoteServiceProxy[MessageType], dest: RemoteServiceProxy[MessageType]) extends Runnable {
    def run() {
      dest.!(msg)(sender)
    }
  }

  def !!!(msg: MessageType)(implicit sender: RemoteServiceProxy[MessageType]): Unit = {
    msgQueue.synchronized {
      sendExecutor.execute(new SendRequest(msg, sender, this))
    }
  }

  /*
  private var msgSet = List[(MessageType, MessageFuture[MessageType])]()

  /**
   * Collects messages for bulk-send
   */
  def !!!(body: MessageType): MessageFuture[MessageType] = {
    val future = new MessageFuture[MessageType]
    synchronized {
      msgSet ::= Tuple2(body, future)
    }
    return future
  }

  /*
  def commit(): Unit = synchronized {
    val currentSet = msgSet
    msgSet = Nil

    if (currentSet.length == 1) {
      logger.debug("Falling back to normal send for commit size 1")
      this.!(currentSet.head._1)(currentSet.head._2.remoteService)
    }
    else if (currentSet.length > 1) {
      val batchFuture = !!(new BatchRequest(currentSet.map(_._1)))
      batchFuture.respond {
        case BatchResponse(ranges) => {
          logger.trace("Unpacking %d messages to %d futures", ranges.length, currentSet.length)
          ranges.zip(currentSet.map(_._2)).foreach {
            case (msg, subFuture) => subFuture.receiveMessage(batchFuture.source, msg)
          }
        }
        case unexp => {
          logger.warning("Batch request failed with unexpected message: %s", unexp)
          currentSet.foreach(_._2.receiveMessage(batchFuture.source, unexp))
        }
      }
    }
  }
  */
  */
}

