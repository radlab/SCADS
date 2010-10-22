package edu.berkeley.cs.scads.comm

import scala.actors._
import scala.actors.Actor._

import net.lag.logging.Logger

import edu.berkeley.cs.avro.marker.AvroRecord
import edu.berkeley.cs.avro.marker.AvroUnion
import collection.mutable.ArrayBuffer
import concurrent.{Lock, SyncVar}

object Actors {
  import scala.actors._
  import scala.actors.Actor._

  /* TODO: link to the created actor and unregister on exit */
  def remoteActor(body: (RemoteActorProxy) => Unit): Actor = {
    val a = new Actor() {
      def act(): Unit = {
        val ra = MessageHandler.registerActor(self)
        body(ra)
        MessageHandler.unregisterActor(ra)
      }
    }
    a.start
    return a
  }
}

/* Generic Remote Actor Handle */
case class RemoteActor(var host: String, var port: Int, var id: ActorId) extends RemoteActorProxy with AvroRecord

/* Specific types for different services. Note: these types are mostly for readability as typesafety isn't enforced when serialized individualy*/
case class StorageService(var host: String, var port: Int, var id: ActorId) extends RemoteActorProxy with AvroRecord
case class PartitionService(var host: String, var port: Int, var id: ActorId, var partitionId: String, var storageService:StorageService) extends RemoteActorProxy with AvroRecord

case class TimeoutException(msg: MessageBody) extends Exception

/* This is a placeholder until stephen's remote actor handles are available */
trait RemoteActorProxy {
  var host: String
  var port: Int
  var id: ActorId

  def remoteNode = RemoteNode(host, port)

  override def toString(): String = id + "@" + host + ":" + port

  /**
   * Returns an ouput proxy that forwards any messages to the remote actor with and empty sender.
   */
  def outputChannel = new OutputChannel[Any] {
    def !(msg: Any):Unit = msg match {
      case msgBody: MessageBody => MessageHandler.sendMessage(remoteNode, Message(None, id, None, msgBody))
      case otherMessage => throw new RuntimeException("Invalid remote message type:" + otherMessage + " Must extend MessageBody.")
    }
    def forward(msg: Any):Unit = throw new RuntimeException("Unimplemented")
    def send(msg: Any, sender: OutputChannel[Any]):Unit = throw new RuntimeException("Unimplemented")
    def receiver: Actor = throw new RuntimeException("Unimplemented")
  }

  /**
   * Send a message asynchronously.
   **/
  def !(body: MessageBody)(implicit sender: RemoteActorProxy): Unit = {
    MessageHandler.sendMessage(remoteNode, Message(Some(sender.id), id, None, body))
  }

  /**
   * Send a message and synchronously wait for a response.
   */
  def !?(body: MessageBody, timeout: Int = 5000): MessageBody = {
      val future = new MessageFutureImpl
      this.!(body)(future.remoteActor)
      future.get(timeout) match {
        case Some(exp: RemoteException) => throw new RuntimeException(exp.toString)
        case Some(msg: MessageBody) => msg
        case None => {
          future.cancel
          throw TimeoutException(body)
        }
      }
  }

  /**
   * Sends a message and returns a future for the response.
   */
  def !!(body: MessageBody): MessageFuture = {
    val future = new MessageFutureImpl
    this.!(body)(future.remoteActor)
    future
  }

  private var msgSet = new ArrayBuffer[(MessageBody, BatchFuture)]()

  /**
   * Collects messages for bulk-send
   */
  def !!!(body : MessageBody) : MessageFuture = {
    var future : BatchFuture = null
    msgSet.synchronized{
      val i: Int = msgSet.length
      future = new BatchFuture(i)
      msgSet += Tuple2(body, future)
    }
    return future
  }

  def commit() : Unit = {
    msgSet.synchronized{
      if(msgSet.length == 0){
        return
      }
      val future = if(msgSet.length == 1) !!(msgSet(0)._1) else !!(new BatchRequest(msgSet.map(_._1)))
      msgSet.map(_._2.future = future)
      msgSet.clear()
    }
  }

  /* Handle type conversion methods.  Note: Not typesafe obviously */
  def toPartitionService(partitionId : String, storageService : StorageService): PartitionService = new PartitionService(host, port, id, partitionId, storageService)
  def toStorageService: StorageService = new StorageService(host, port, id)
}

