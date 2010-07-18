package edu.berkeley.cs.scads.comm

import scala.concurrent.SyncVar
import scala.actors._
import scala.actors.Actor._

import org.apache.log4j.Logger

import com.googlecode.avro.marker.AvroRecord
import com.googlecode.avro.annotation.AvroUnion

/* This is a placeholder until stephen's remote actor handles are available */
case class RemoteActor(var host: String, var port: Int, var id: ActorId) extends AvroRecord {
  def !(body: MessageBody)(implicit sender: RemoteActor): Unit = {
    MessageHandler.sendMessage(RemoteNode(host, port), Message(sender.id, id, None, body))
  }

  def !?(body: MessageBody, timeout: Int = 5000): MessageBody = {
		val resp = new SyncVar[Either[Throwable, MessageBody]]
    val a = actor {
      val srcId = ActorNumber(MessageHandler.registerActor(self))
      MessageHandler.sendMessage(RemoteNode(host, port), Message(srcId, id, None, body))

      reactWithin(timeout) {
        case (RemoteNode(hostname, port), msg: Message) => msg.body match {
          case exp: ProcessingException => resp.set(Left(new RuntimeException("Remote Exception" + exp)))
          case obj: MessageBody => resp.set(Right(obj))
        }
        case TIMEOUT => resp.set(Left(new RuntimeException("Timeout")))
        case msg => println("Unexpected message: " + msg)
      }
      MessageHandler.unregisterActor(srcId.num)
    }

    resp.get match {
      case Right(msg) => msg
      case Left(exp) => throw exp
    }
  }
}
