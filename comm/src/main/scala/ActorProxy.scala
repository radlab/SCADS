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

class StorageEchoPrintServer extends StorageEchoServer {
    val lock = new Object
    var numMsgs = 0
    override def receiveMessage(src: RemoteNode, req: StorageRequest): Unit = {
        lock.synchronized {
            numMsgs += 1
            if (numMsgs % 100000 == 0) println("On msg: " + numMsgs)
        }
        super.receiveMessage(src, req)
	}
}

class StorageDiscardServer extends NioAvroChannelManagerBase[StorageResponse, StorageRequest] {
	def receiveMessage(src: RemoteNode, req: StorageRequest): Unit = { }
}


class StorageActorProxy extends NioAvroChannelManagerBase[StorageRequest, StorageResponse] {
	def receiveMessage(src: RemoteNode, msg: StorageResponse): Unit = ActorRegistry.sendMessage(msg.dest, (src, msg))
}
