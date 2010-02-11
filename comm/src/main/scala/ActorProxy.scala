package edu.berkeley.cs.scads.comm

import java.lang.ref.WeakReference
import java.lang.ref.ReferenceQueue
import java.util.concurrent.ConcurrentHashMap

import scala.actors._

import org.apache.log4j.Logger

class StorageEchoServer extends NioAvroChannelManagerBase[StorageResponse, StorageRequest] {
	def receiveMessage(src: RemoteNode, req: StorageRequest): Unit = {
		val resp = new StorageResponse
		resp.dest = req.src
		sendMessage(src, resp)
	}
}

class StorageActorProxy extends NioAvroChannelManagerBase[StorageRequest, StorageResponse] {
	def receiveMessage(src: RemoteNode, msg: StorageResponse): Unit = ActorRegistry.sendMessage(msg.dest, (src, msg))
}
