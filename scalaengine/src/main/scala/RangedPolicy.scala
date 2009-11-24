package edu.berkeley.cs.scads.storage

import java.io.ByteArrayInputStream
import java.io.ObjectInputStream
import java.io.ByteArrayOutputStream
import java.io.ObjectOutputStream

object RangedPolicy {
	def apply(policy: Array[Byte]): RangedPolicy = {
		val bytes = new ByteArrayInputStream(policy)
		val objectIn = new ObjectInputStream(bytes)

		new RangedPolicy(objectIn.readObject().asInstanceOf[Array[(String, String)]])
	}
}

class RangedPolicy(policy: Array[(String, String)]) {

	def contains(key: String): Boolean = {
		policy.foreach(p => {
			if((p._1 == null || p._1.compare(key) <= 0) && (p._2 == null || p._2.compare(key) >= 0))
				return true
		})
		return false
	}

	def getBytes(): Array[Byte] = {
		val bytes = new ByteArrayOutputStream()
		val objectOut = new ObjectOutputStream(bytes)

		objectOut.writeObject(policy)
		bytes.toByteArray()
	}
}
