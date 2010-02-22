package edu.berkeley.cs.scads.comm

import scala.actors._
import scala.actors.Actor._
import scala.concurrent.SyncVar

import org.apache.log4j.Logger

object Sync {
	val logger = Logger.getLogger("scads.comm.sync")

	def makeRequest(rn: RemoteNode, dest: Object, reqBody: Object): Object = {
		val resp = new SyncVar[Either[Throwable, Object]]

		val a = actor {
			val req = new Message
			req.body = reqBody
      req.dest = dest
			req.src = new java.lang.Long(MessageHandler.registerActor(self))
			MessageHandler.sendMessage(rn, req)
			reactWithin(10000) {
				case (RemoteNode(hostname, port), msg: Message) => msg.body match {
					case exp: ProcessingException => resp.set(Left(new RuntimeException("Remote Exception" + exp)))
					case obj => resp.set(Right(obj))
				}
				case TIMEOUT => resp.set(Left(new RuntimeException("Timeout")))
				case msg => logger.warn("Unexpected message: " + msg)
			}
		}

		resp.get match {
			case Right(msg) => msg
			case Left(exp) => throw exp
		}
	}
}
