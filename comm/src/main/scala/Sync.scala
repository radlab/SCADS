package edu.berkeley.cs.scads.comm

import scala.actors._
import scala.actors.Actor._
import scala.concurrent.SyncVar

import org.apache.log4j.Logger

object Sync {
	val logger = Logger.getLogger("scads.comm.sync")

	def makeRequest(dest: RemoteNode, reqBody: Object)(implicit mgr: StorageActorProxy): StorageResponse = {
		val resp = new SyncVar[Either[Throwable, StorageResponse]]

		val a = actor {
			val req = new StorageRequest
			req.body = reqBody
			req.src = ActorRegistry.registerActor(self)
			mgr.sendMessage(dest, req)

			reactWithin(10000) {
				case (RemoteNode(hostname, port), msg: StorageResponse) => resp.set(Right(msg))
				case unexp: ProcessingException => resp.set(Left(new RuntimeException("Remote Exception" + unexp)))
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
