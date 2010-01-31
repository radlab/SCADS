package edu.berkeley.cs.scads.comm

import org.apache.avro.Schema
import org.apache.avro.generic._
import org.apache.avro.util.Utf8

object Conversions {
	class ScalaContainer[RecType <: GenericContainer](base: List[RecType]) extends GenericArray[RecType]{
		class IterWrapper[T](iter: Iterator[T]) extends java.util.Iterator[T] {
			def hasNext(): Boolean = iter.hasNext
			def next(): T = iter.next
			def remove(): Unit = iter.next
		}

		def getSchema(): Schema = Schema.createArray(base.first.getSchema())
		def iterator(): java.util.Iterator[RecType] = new IterWrapper(base.elements)
		def peek(): RecType = base.last
		def add(elem: RecType): Unit = null
		def size():Long = base.size
		def clear(): Unit = null
	}

	implicit def mkArray[RecType <: GenericContainer](base: List[RecType]): GenericArray[RecType] = new ScalaContainer(base)
	implicit def toUtf8(str: String): Utf8 = new Utf8(str)
}
