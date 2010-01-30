package edu.berkeley.cs.scads.comm

import org.specs._
import org.specs.runner.JUnit4
import scala.collection.jcl.Conversions
import org.apache.log4j.BasicConfigurator

object BufferSpec extends SpecificationWithJUnit("Circular buffer spec") {

    "a circular buffer" should {
        "expand properly" >> {
            val buf = new CircularByteBuffer(4)

            val a1 = (1 to buf.bytesRemaining*2).map(_.toByte) 
            buf.append( a1.toArray )
            buf.size must_== a1.length 

            val a2 = (1 to buf.bytesRemaining*2).map(_.toByte) 
            buf.append( a2.toArray ) 
            buf.size must_== a1.length + a2.length

            val a3 = (1 to buf.bytesRemaining*3).map(_.toByte) 
            buf.append( a3.toArray ) 
            buf.size must_== a1.length + a2.length + a3.length
        }

        "consume bytes properly" >> {
            val buf = new CircularByteBuffer(4)

            val a1 = (1 to 16).map(_.toByte) 
            buf.append( a1.toArray )
            
            val a2 = buf.consumeBytes(3)
            buf.size must_== a1.length - a2.length
            a2.toList.equals((1 to 3).map(_.toByte).toList) must_== true

            val a3 = buf.consumeBytes(7)
            buf.size must_== a1.length - a2.length - a3.length
            a3.toList.equals((4 to 10).map(_.toByte).toList) must_== true

            val a4 = buf.consumeBytes(6)
            buf.isEmpty must_== true
            a4.toList.equals((11 to 16).map(_.toByte).toList) must_== true
        }

        "interleave consume and append properly" >> {
            val buf = new CircularByteBuffer(9)
        
            val a1 = (1 to 9).map(_.toByte)
            buf.append( a1.toArray )
            buf.size must_== a1.length

            val a2 = buf.consumeBytes(5)
            buf.size must_== a1.length - a2.length
            a2.toList.equals((1 to 5).map(_.toByte).toList) must_== true

            val a3 = (10 to 14).map(_.toByte)
            buf.append( a3.toArray )
            buf.size must_== 9

            val a4 = buf.consumeBytes(7)
            buf.size must_== 2
            a4.toList.equals((6 to 12).map(_.toByte).toList) must_== true

        }

        "consume ints properly" >> {
            import java.nio.ByteBuffer
            val buf = new CircularByteBuffer(1)
            val bb = ByteBuffer.allocate(4)
            bb.putInt(0xabcdef12)
            buf.append( bb.array )
            buf.size must_== 4
            val a1 = buf.consumeInt
            a1 must_== 0xabcdef12 
        }

    }

}

class BufferSpecTest extends JUnit4(BufferSpec)
