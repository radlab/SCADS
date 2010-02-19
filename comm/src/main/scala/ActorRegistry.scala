package edu.berkeley.cs.scads.comm

import scala.actors._

import org.apache.log4j.Logger

import java.lang.ref.WeakReference
import java.lang.ref.ReferenceQueue
import java.util.concurrent.ConcurrentHashMap

import scala.util.Random

class ActorWeakReference(var actor: Actor, val _queue: ReferenceQueue[Actor], val uniqId:Long) extends WeakReference[Actor](actor, _queue) {
    actor = null
}

object ActorRegistry {
    val logger = Logger.getLogger("scads.actorProxy")
	val registry = new ConcurrentHashMap[Long, ActorWeakReference]
	val deadActors = new ReferenceQueue[Actor]()
    val rand = new Random

	val actorCleanupThread = new Thread() {
		override def run(): Unit = {
			while(true) {
				val ref = deadActors.remove()
				logger.debug("Garbage collecting actor registration.")
				val success = registry.remove(ref.asInstanceOf[ActorWeakReference].uniqId)
                logger.debug("Was able to remove successfully? " + success)
			}
		}
	}

	def registerActor(a: Actor): Long = {
        var uniqId = rand.nextLong 
        synchronized {
            if (registry.size >= Math.MAX_LONG)
                throw new IllegalStateException("Too many actors!")
            while (registry.containsKey(uniqId)) uniqId = rand.nextLong
            val ref = new ActorWeakReference(a, deadActors, uniqId)
            registry.put(uniqId, ref)
        }
        uniqId
	}
  
    def sendMessage(id: Long, msg: Object): Unit = {
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
