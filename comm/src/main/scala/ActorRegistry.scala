package edu.berkeley.cs.scads.comm

import scala.actors._

import org.apache.log4j.Logger

import java.lang.ref.WeakReference
import java.lang.ref.ReferenceQueue
import java.util.concurrent.ConcurrentHashMap

object ActorRegistry {
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
  
  def sendMessage(id: Int, msg: Object): Unit = {
    val targetRef = registry.get(id)
		if(targetRef == null) {
			logger.warn("Message received for unknown actor. " + id)
			return
		}

		val targetActor = targetRef.get()
		if(targetActor == null) {
			logger.warn("Message received for actor that has been gc-ed. " + id)
			return
		}

		targetActor ! msg
  }
}
