package edu.berkeley.cs.scads.comm

import java.io._

object Closure {
	def apply(cl: Array[Byte]): () => Unit = {
		val buff = new ByteArrayInputStream(cl)
		val s = new ObjectInputStream(buff)
		s.readObject.asInstanceOf[Function0[Unit]]
	}
}

class Closure(cl: () => Unit) {
	def toBytes() = {
		val buff = new ByteArrayOutputStream()
		val s = new ObjectOutputStream(buff);
		s.writeObject(cl)
		buff.toByteArray
	}
}
