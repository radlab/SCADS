package edu.berkeley.cs.scads.model

import scala.collection.mutable.HashMap
import org.apache.log4j.Logger

/**
 * Used as a store of tuples either seen or written by the user to enforce Session Guarantees.
 */
abstract class Session {
	val logger = Logger.getLogger("scads.session")

	/**
	 * Add a set of (namespace, key, version, value) tuples to the store.
	 */
	def addWrites(writes: Seq[(String, Field, Version, String)])

	/**
	 * Get single relevant tuple by key
	 * @returns a tuple or (key, null, null) if none exists
	 */
	def get(namespace: String, key: Field): (Field, Version, String)

	/**
	 * Get relevant range of tuples
	 * @returns a Seq[tuple] or and empty sequence if none exist
	 */
	def get(namespace: String, start: Field, end: Field, offset: Int, limit: Int): Seq[(Field, Version, String)]
}

/**
 * Session store to be used for testing, keeps everything in memory.
 * TODO: implement ranged get
 */
class TrivialSession extends Session {
	type KeyStore = HashMap[String, (Field, Version, String)]
	val writeStore = new HashMap[String, KeyStore]

	private def getKeyStore(namespace: String): KeyStore = {
		writeStore.get(namespace) match {
			case m: Some[KeyStore] => m.get
			case None => {
				val nMap = new KeyStore()
				writeStore.put(namespace, nMap)
				nMap
			}
		}
	}

	def addWrites(writes: Seq[(String, Field, Version, String)]) = {
		logger.debug("Adding writes to session:" + writes)

		writes.foreach((w) => getKeyStore(w._1).put(w._2.serializeKey, (w._2, w._3, w._4)))
	}

	def get(namespace: String, key: Field): (Field, Version, String) = {
		logger.debug("lookup done to session" + writeStore)
		getKeyStore(namespace).getOrElse(key.serializeKey, (key, null, null))
	}
	def get(namespace: String, start: Field, end: Field, offset: Int, limit: Int): Seq[(Field, Version, String)] = Array()
}
