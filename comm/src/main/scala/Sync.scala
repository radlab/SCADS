package edu.berkeley.cs.scads.comm

import scala.actors.Actor._
import scala.concurrent.SyncVar

object Sync {
	def makeRequest(dest: RemoteNode, req: StorageRequest)(implicit mgr: StorageActorProxy): StorageResponse = {
		val resp = new SyncVar[StorageResponse]

		val a = actor {
			req.src = mgr.registerActor(self)
			mgr.sendMessage(dest, req)

			reactWithin(1000) {
				case msg: StorageResponse => resp.set(msg)
			}
		}

		resp.get
	}
}
