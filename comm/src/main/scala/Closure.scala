package edu.berkeley.cs.scads.comm

import java.io._
import java.nio.ByteBuffer
import org.apache.avro.specific.SpecificRecordBase


object Closure {
	def apply(buff: InputStream): Object = {
		val s = new ObjectInputStream(buff)
		s.readObject
	}

  def apply(cl: Function0[_]): Array[Byte] = {
		val buff = new ByteArrayOutputStream()
		val s = new ObjectOutputStream(buff);
		s.writeObject(cl)
		buff.toByteArray
  }

  def apply(cl: Function1[_,_]): Array[Byte] = {
		val buff = new ByteArrayOutputStream()
		val s = new ObjectOutputStream(buff);
		s.writeObject(cl)
		buff.toByteArray
  }

  def apply(cl: Function2[_,_,_]): Array[Byte] = {
		val buff = new ByteArrayOutputStream()
		val s = new ObjectOutputStream(buff);
		s.writeObject(cl)
		buff.toByteArray
  }
}
