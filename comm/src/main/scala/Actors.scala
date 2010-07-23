package edu.berkeley.cs.scads.comm

import scala.concurrent.SyncVar
import scala.actors._
import scala.actors.Actor._

import org.apache.log4j.Logger

import com.googlecode.avro.marker.AvroRecord
import com.googlecode.avro.annotation.AvroUnion

object Actors {
  import scala.actors._
  import scala.actors.Actor._

  /* TODO: link to the created actor and unregister on exit */
  def remoteActor(body: (RemoteActor) => Unit): Actor = {
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
case class PartitionService(var host: String, var port: Int, var id: ActorId) extends RemoteActorProxy with AvroRecord

/* This is a placeholder until stephen's remote actor handles are available */
trait RemoteActorProxy {
  var host: String
  var port: Int
  var id: ActorId

  def toRemoteNode = RemoteNode(host, port)

  def outputChannel = new OutputChannel[Any] {
    def !(msg: Any):Unit = msg match {
      case msgBody: MessageBody => MessageHandler.sendMessage(toRemoteNode, Message(None, id, None, msgBody))
      case _ => throw new RuntimeException("Invalid remote message.  Must extend MessageBody.")
    }
    def forward(msg: Any):Unit = throw new RuntimeException("Unimplemented")
    def send(msg: Any, sender: OutputChannel[Any]):Unit = throw new RuntimeException("Unimplemented")
    def receiver: Actor = throw new RuntimeException("Unimplemented")
  }

  def !(body: MessageBody)(implicit sender: RemoteActorProxy): Unit = {
    MessageHandler.sendMessage(toRemoteNode, Message(Some(sender.id), id, None, body))
  }

  def !?(body: MessageBody, timeout: Long = 5000): MessageBody = {
		val resp = new SyncVar[Either[Throwable, MessageBody]]
    val a = actor {
      val srcId = MessageHandler.registerActor(self)
      MessageHandler.sendMessage(toRemoteNode, Message(Some(srcId.id), id, None, body))

      reactWithin(timeout) {
        case exp: ProcessingException => resp.set(Left(new RuntimeException("Remote Exception" + exp)))
        case obj: MessageBody => resp.set(Right(obj))
        case TIMEOUT => resp.set(Left(new RuntimeException("Timeout")))
        case msg => println("Unexpected message: " + msg)
      }
      MessageHandler.unregisterActor(srcId)
    }

    resp.get match {
      case Right(msg) => msg
      case Left(exp) => throw exp
    }
  }

  def !!(body: MessageBody): Future[MessageBody] = throw new RuntimeException("Unimplemented")

  /* Handle type conversion methods.  Note: Not typesafe */
  def toPartitionService: PartitionService = new PartitionService(host, port, id)
  def toStorageService: StorageService = new StorageService(host, port, id)
}


