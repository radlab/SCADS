package edu.berkeley.cs.scads.comm

import java.nio.ByteBuffer

object EncoderTest {

    def encodeByteBuffer1(data: ByteBuffer): ByteBuffer = {
        val newBuffer = ByteBuffer.allocate(4 + data.remaining)
        newBuffer.putInt(data.remaining) 
        newBuffer.put(data)
        newBuffer.rewind
        newBuffer
    }

    def encodeByteBuffer2(data: ByteBuffer): ByteBuffer = {
        val buf = new Array[Byte](4 + data.remaining)
        buf(0) = ((data.remaining >> 24) & 0xFF).toByte 
        buf(1) = ((data.remaining >> 16) & 0xFF).toByte 
        buf(2) = ((data.remaining >> 8) & 0xFF).toByte
        buf(3) = (data.remaining & 0xFF).toByte
        data.get(buf, 4, data.remaining)
        ByteBuffer.wrap(buf)
    }

    def main(args: Array[String]):Unit = {
        val start1 = System.currentTimeMillis
        (1 to 1000000).foreach( i => {
            val b = ByteBuffer.allocate(24)
            b.put(new Array[Byte](24))
            b.rewind
            encodeByteBuffer1(b)
        })
        val end1 = System.currentTimeMillis

        val start2 = System.currentTimeMillis
        (1 to 1000000).foreach( i => {
            val b = ByteBuffer.allocate(24)
            b.put(new Array[Byte](24))
            b.rewind
            encodeByteBuffer2(b)
        })
        val end2 = System.currentTimeMillis

        println("TIME 1: " + (end1-start1))
        println("TIME 2: " + (end2-start2))
    }

}
