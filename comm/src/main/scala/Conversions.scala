package edu.berkeley.cs.scads.comm

import scala.collection.mutable.ListBuffer
import org.apache.avro.Schema
import org.apache.avro.generic._
import org.apache.avro.util.Utf8
import java.io.InputStream
import java.nio.ByteBuffer

object Conversions {
	class ScalaContainer[RecType <: GenericContainer](base: List[RecType]) extends GenericArray[RecType]{
		class IterWrapper[T](iter: Iterator[T]) extends java.util.Iterator[T] {
			def hasNext(): Boolean = iter.hasNext
			def next(): T = iter.next
			def remove(): Unit = iter.next
		}

		def getSchema(): Schema = Schema.createArray(base.head.getSchema())
		def iterator(): java.util.Iterator[RecType] = new IterWrapper(base.iterator)
		def peek(): RecType = base.last
		def add(elem: RecType): Unit = null
		def size():Long = base.size
		def clear(): Unit = null
	}

	implicit def mkArray[RecType <: GenericContainer](base: List[RecType]): GenericArray[RecType] = new ScalaContainer(base)
    implicit def genericArray2ScalaList[T](gen: GenericArray[T]): List[T] = {
        val lb = new ListBuffer[T]
        val iter = gen.iterator
        while (iter.hasNext)
            lb += iter.next
        lb.toList
    }
	implicit def mkUtf8(str: String): Utf8 = new Utf8(str)
	implicit def mkString(utf8: Utf8): String = utf8.toString
	implicit def mkByteArray(str: String): Array[Byte] = str match {
        case null => null
        case _    => str.getBytes
    }
	implicit def mkBytes(str: String): ByteBuffer = str match {
        case null => null
        case _    => ByteBuffer.wrap(str.getBytes)
    }
	implicit def mkBytes(bts: Array[Byte]): ByteBuffer = bts match {
        case null => null
        case _    => ByteBuffer.wrap(bts)
    }

  implicit def mkByteInputStream(buff: ByteBuffer): InputStream = {
   new InputStream() {
      override def read(): Int = {
        if(buff.remaining > 0)
          buff.get & 0x00FF
        else
          -1
      }
    }
  }
}
