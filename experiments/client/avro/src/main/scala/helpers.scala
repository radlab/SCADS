package edu.berkeley.cs.scads.test.helpers

import org.apache.avro.Schema
import org.apache.avro.specific.{SpecificRecord, SpecificDatumWriter, SpecificDatumReader}
import org.apache.avro.io.{BinaryEncoder,BinaryDecoder}
import org.apache.avro.ipc.ByteBufferInputStream
import org.apache.avro.util.Utf8
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

    def bytesToMsg[T <: SpecificRecord](bytes: Array[Byte], newInstance: T):T = {
        val msgReader = new SpecificDatumReader[T](newInstance.getSchema)
        val inStream = new BinaryDecoder(new ByteBufferInputStream(java.util.Arrays.asList(ByteBuffer.wrap(bytes))))
        msgReader.read(newInstance, inStream)
    }

}

object Conversions {

    implicit def string2bytebuffer(str: String): ByteBuffer = {
        ByteBuffer.wrap(str.getBytes)
    }

    implicit def string2utf8(str: String):Utf8 = {
        new Utf8(str)
    }

}
