package edu.berkeley.cs.scads.comm

import net.lag.logging.Logger

import edu.berkeley.cs.avro.marker.AvroRecord
import org.apache.avro.generic.IndexedRecord

/* Generic Remote Actor Handle */
case class RemoteService[MessageType <: IndexedRecord](var host: String,
                       var port: Int,
                       var id: ServiceId)
                      (implicit val registry: ServiceRegistry[MessageType])
  extends RemoteServiceProxy[MessageType]

/* Specific types for different services. Note: these types are mostly for readability as typesafety isn't enforced when serialized individualy*/
case class StorageService(var host: String,
                          var port: Int,
                          var id: ServiceId) extends AvroRecord

case class PartitionService(var host: String,
                            var port: Int,
                            var id: ServiceId,
                            var partitionId: String,
                            var storageService: StorageService) extends AvroRecord

case class TimeoutException(msg: IndexedRecord) extends Exception

object RemoteServiceProxy {
  val logger = Logger()
}

trait RemoteServiceProxy[MessageType <: IndexedRecord] {
  var host: String
  var port: Int
  var id: ServiceId

  import RemoteServiceProxy._

  implicit val registry: ServiceRegistry[MessageType]

  def remoteNode = RemoteNode(host, port)

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

  /**
   * Send a message and synchronously wait for a response.
   */
  def !?(msg: MessageType, timeout: Int = 10 * 1000): MessageType = {
    val future = new MessageFuture[MessageType]
    this.!(msg)(future.remoteService)
    future.get(timeout) match {
      //TODO: fix exception throwing: case Some(exp: RemoteException) => throw new RuntimeException(exp.toString)
      case Some(msg) => msg.asInstanceOf[MessageType]
      case None => {
        throw TimeoutException(msg)
      }
    }
  }

  /**
   * Sends a message and returns a future for the response.
   */
  def !!(body: MessageType): MessageFuture[MessageType] = {
    val future = new MessageFuture[MessageType]
    this.!(body)(future.remoteService)
    future
  }

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
}

