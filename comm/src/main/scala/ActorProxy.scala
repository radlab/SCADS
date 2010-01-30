package edu.berkeley.cs.scads.comm

import java.lang.ref.WeakReference
import java.lang.ref.ReferenceQueue
import java.util.concurrent.ConcurrentHashMap

import scala.actors._

import org.apache.log4j.Logger

class StorageEchoServer extends AvroChannelManager[StorageResponse, StorageRequest] {
	def receiveMessage(src: RemoteNode, req: StorageRequest): Unit = {
		val resp = new StorageResponse
		resp.dest = req.src
		sendMessage(src, resp)
	}
}

class StorageActorProxy extends AvroChannelManager[StorageRequest, StorageResponse] {
	val logger = Logger.getLogger("scads.actorProxy")
	val registry = new ConcurrentHashMap[Int, WeakReference[Actor]]
	val deadActors = new ReferenceQueue[Actor]()

	val actorCleanupThread = new Thread() {
		override def run(): Unit = {
			while(true) {
				val ref = deadActors.remove()
				logger.debug("Garbage collecting actor registration.")
				registry.remove(ref.hashCode)
			}
		}
	}

	def registerActor(a: Actor): Int = {
		val ref = new WeakReference(a)
		if(registry.put(ref.hashCode, ref) != null) {
			logger.warn("Registering an actor that was already registered. " + ref.hashCode)
		}

		ref.hashCode
	}

	def receiveMessage(src: RemoteNode, msg: StorageResponse): Unit = {
		val targetRef = registry.get(msg.dest)
		if(targetRef == null) {
			logger.warn("Message received for unknown actor. " + msg.dest)
			return
		}

		val targetActor = targetRef.get()
		if(targetActor == null) {
			logger.warn("Message received for actor that has been gc-ed. " + msg.dest)
			return
		}

		targetActor ! msg
	}
}
