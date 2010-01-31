package edu.berkeley.cs.scads.comm

import scala.actors._
import scala.actors.Actor._
import scala.concurrent.SyncVar

object Sync {
	def makeRequest(dest: RemoteNode, reqBody: Object)(implicit mgr: StorageActorProxy): StorageResponse = {
		val resp = new SyncVar[Either[Throwable, StorageResponse]]

		val a = actor {
			val req = new StorageRequest
			req.body = reqBody
			req.src = ActorRegistry.registerActor(self)
			mgr.sendMessage(dest, req)

			reactWithin(1000) {
				case msg: StorageResponse => resp.set(Right(msg))
				case unexp: ProcessingException => resp.set(Left(new RuntimeException("Remote Exception" + unexp)))
				case TIMEOUT => resp.set(Left(new RuntimeException("Timeout")))
			}
		}

		resp.get match {
			case Right(msg) => msg
			case Left(exp) => throw exp
		}
	}
}
