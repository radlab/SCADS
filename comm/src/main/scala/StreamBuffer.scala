package edu.berkeley.cs.scads.comm

import java.io.OutputStream
import java.nio.ByteBuffer

class ByteBufferOutputStream extends OutputStream {
	val size = ByteBuffer.allocate(4)
	val buffer = ByteBuffer.allocate(8192)
	val data = Array(size, buffer)

	def clear() = buffer.clear

	def getData(): Array[ByteBuffer] = {
		buffer.flip
		size.clear()
		size.putInt(buffer.limit)
		size.rewind
		data
	}

	override def write(b: Int): Unit = {
		buffer.put(b.asInstanceOf[Byte])
	}

	override def write(b: Array[Byte], off: Int, len: Int): Unit = {
		buffer.put(b, off, len)
	}
}
