package edu.berkeley.cs.scads.comm

import scala.actors._
import scala.actors.Actor._
import scala.concurrent.SyncVar

import org.apache.log4j.Logger
import org.apache.avro.util.Utf8

object Sync {
	val logger = Logger.getLogger("scads.comm.sync")

	def makeRequest(rn: RemoteNode, dest: ActorId, reqBody: MessageBody): Object =
    makeRequest(rn,dest,reqBody,10000)

	def makeRequest(rn: RemoteNode, dest: ActorId, reqBody: MessageBody, timeout:Long): Object = {
		val resp = new SyncVar[Either[Throwable, Object]]

		val a = actor {
			val req = classOf[Message].newInstance() //HACK
			req.body = reqBody
      req.dest = dest
      val id = MessageHandler.registerActor(self)
      req.src = ActorNumber(id)
			MessageHandler.sendMessage(rn, req)
			reactWithin(timeout) {
				case (RemoteNode(hostname, port), msg: Message) => msg.body match {
					case exp: ProcessingException => resp.set(Left(new RuntimeException("Remote Exception" + exp)))
					case obj => resp.set(Right(obj))
				}
				case TIMEOUT => resp.set(Left(new RuntimeException("Timeout")))
				case msg => logger.warn("Unexpected message: " + msg)
			}
      MessageHandler.unregisterActor(id)
		}

		resp.get match {
			case Right(msg) => msg
			case Left(exp) => throw exp
		}
	}

	def makeRequestNoTimeout(rn: RemoteNode, dest: Object, reqBody: MessageBody): Object = {
		val resp = new SyncVar[Either[Throwable, Object]]

		val a = actor {
			val req = classOf[Message].newInstance() //HACK
			req.body = reqBody
      req.dest = dest match {
        case l: java.lang.Long => new ActorNumber(l.asInstanceOf[Long]) // TODO: is this the right type of long we want?
        case s: String         => new ActorName(s)
        case u: Utf8           => new ActorName(u.toString)
        case _                 => throw new IllegalArgumentException("bad dest obj")
      }

      val id = MessageHandler.registerActor(self)
      req.src = ActorNumber(id)
			MessageHandler.sendMessage(rn, req)
			react {
				case (RemoteNode(hostname, port), msg: Message) => msg.body match {
					case exp: ProcessingException => resp.set(Left(new RuntimeException("Remote Exception" + exp)))
					case obj => resp.set(Right(obj))
				}
				case TIMEOUT => resp.set(Left(new RuntimeException("Timeout")))
				case msg => logger.warn("Unexpected message: " + msg)
			}
      MessageHandler.unregisterActor(id)
		}

		resp.get match {
			case Right(msg) => msg
			case Left(exp) => throw exp
		}
	}
}
