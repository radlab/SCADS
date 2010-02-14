package edu.berkeley.cs.scads.test.helpers

import org.apache.avro.specific.{SpecificRecord, SpecificDatumWriter}
import org.apache.avro.io.BinaryEncoder
import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer


object Helpers {

    def msgToBytes[T <: SpecificRecord](msg: T):Array[Byte] = {
        val msgWriter = new SpecificDatumWriter[T](msg.getSchema)
        val buffer = new ByteArrayOutputStream(128)
        val encoder = new BinaryEncoder(buffer)
        msgWriter.write(msg, encoder)
        buffer.toByteArray
    }

}

object Conversions {

    implicit def string2bytebuffer(str: String): ByteBuffer = {
        ByteBuffer.wrap(str.getBytes)
    }

}
