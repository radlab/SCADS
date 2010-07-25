package edu.berkeley.cs.scads.comm.test

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.Spec
import org.scalatest.matchers.ShouldMatchers

import edu.berkeley.cs.scads.comm._

@RunWith(classOf[JUnitRunner])
class BufferSpec extends Spec with ShouldMatchers {

    describe("a circular buffer") {
        it("should expand properly") {
            val buf = new CircularByteBuffer(4)

            val a1 = (1 to buf.bytesRemaining*2).map(_.toByte)
            buf.append( a1.toArray )
            buf.size should equal( a1.length )

            val a2 = (1 to buf.bytesRemaining*2).map(_.toByte)
            buf.append( a2.toArray )
            buf.size should equal( a1.length + a2.length )

            val a3 = (1 to buf.bytesRemaining*3).map(_.toByte)
            buf.append( a3.toArray )
            buf.size should equal( a1.length + a2.length + a3.length )
        }

        it("should consume bytes properly") {
            val buf = new CircularByteBuffer(4)

            val a1 = (1 to 16).map(_.toByte)
            buf.append( a1.toArray )

            val a2 = buf.consumeBytes(3)
            buf.size should equal( a1.length - a2.length )
            a2.toList.equals((1 to 3).map(_.toByte).toList) should equal( true )

            val a3 = buf.consumeBytes(7)
            buf.size should equal( a1.length - a2.length - a3.length )
            a3.toList.equals((4 to 10).map(_.toByte).toList) should equal( true )

            val a4 = buf.consumeBytes(6)
            buf.isEmpty should equal( true )
            a4.toList.equals((11 to 16).map(_.toByte).toList) should equal( true )
        }

        it("should interleave consume and append properly") {
            val buf = new CircularByteBuffer(9)

            val a1 = (1 to 9).map(_.toByte)
            buf.append( a1.toArray )
            buf.size should equal( a1.length )

            val a2 = buf.consumeBytes(5)
            buf.size should equal( a1.length - a2.length )
            a2.toList.equals((1 to 5).map(_.toByte).toList) should equal( true )

            val a3 = (10 to 14).map(_.toByte)
            buf.append( a3.toArray )
            buf.size should equal( 9 )

            val a4 = buf.consumeBytes(7)
            buf.size should equal( 2 )
            a4.toList.equals((6 to 12).map(_.toByte).toList) should equal( true )

        }

        it("should consume ints properly") {
            import java.nio.ByteBuffer
            val buf = new CircularByteBuffer(1)
            val bb = ByteBuffer.allocate(4)
            bb.putInt(0xabcdef12)
            buf.append( bb.array )
            buf.size should equal( 4 )
            val a1 = buf.consumeInt
            a1 should equal( 0xabcdef12 )
        }

    }

}
