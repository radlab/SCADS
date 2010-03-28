import localhost.test._

import java.io.{ByteArrayInputStream,ByteArrayOutputStream,ObjectInputStream,ObjectOutputStream,Serializable}
import java.nio.ByteBuffer

import org.apache.avro.io._
import org.apache.avro.specific._
import org.apache.avro.util.Utf8

trait Tester {
    val decoderFactory = new DecoderFactory

    def serialize(i: Int):Array[Byte]
    def deserialize(i: Int, bytes: Array[Byte]):Unit

    def messageSize = serialize(0).length 

    def runTest(times: Int):(Long,Long) = {
        var serializeTime = 0L
        var deserializeTime = 0L
        (1 to times).foreach(i => {
            val startSerial = System.currentTimeMillis
            val bytes = serialize(i) 
            val endSerial = System.currentTimeMillis

            val startDeserial = System.currentTimeMillis
            deserialize(i, bytes)
            val endDeserial = System.currentTimeMillis

            serializeTime += endSerial - startSerial
            deserializeTime += endDeserial - startDeserial
        })
        (serializeTime, deserializeTime)
    }

    def avroToByteArray[ T <: SpecificRecord ](record: T) = {
        val writer = new SpecificDatumWriter[T](record.getSchema)
        val buffer = new ByteArrayOutputStream(1024)
        val encoder = new BinaryEncoder(buffer)
        writer.write(record, encoder) 
        buffer.toByteArray
    }

    def avroFromByteArray[ T <: SpecificRecord ](newInstance: T, bytes: Array[Byte]):T = {
        val reader = new SpecificDatumReader[T](newInstance.getSchema)
        val inStream = decoderFactory.createBinaryDecoder(bytes, null)
        reader.read(newInstance, inStream)
        newInstance
    }

    def javaToByteArray[ T <: Serializable ](record: T) = {
        val buffer = new ByteArrayOutputStream(1024)
        val oos = new ObjectOutputStream(buffer)
        oos.writeObject(record)
        oos.flush
        oos.close
        buffer.toByteArray
    }

    def javaFromByteArray[ T <: Serializable ](bytes: Array[Byte]): T = {
        val buffer = new ByteArrayInputStream(bytes)
        val ois = new ObjectInputStream(buffer)
        ois.readObject.asInstanceOf[T]
    }
}

class AvroStockCompilerSerializeTester extends Tester {
    def serialize(i: Int) = {
        val obj = new RecordSC
        obj.x = i
        obj.y = new Utf8("__SerializeMe__")
        obj.z = true
        avroToByteArray(obj)
    }

    def deserialize(i: Int, bytes: Array[Byte]) {
        val obj = avroFromByteArray(new RecordSC, bytes)
        assert(obj.x == i)
    }
}

class AvroScalaCompilerSerializeTester extends Tester {
    def serialize(i: Int) = {
        val obj = RecordASC (i, "__SerializeMe__", true)
        avroToByteArray(obj)
    }
    
    def deserialize(i: Int, bytes: Array[Byte]) {
        val obj = avroFromByteArray(RecordASC (0, "", false), bytes)
        assert(obj.x == i)
    }
}

class JavaSerializeTester extends Tester {
    def serialize(i: Int) = {
        val obj = RecordJS (i, "__SerializeMe__", true)
        javaToByteArray(obj)
    }

    def deserialize(i: Int, bytes: Array[Byte]) {
        val obj = javaFromByteArray[RecordJS](bytes)
        assert(obj.x == i)
    }
}

val avroStockCompSerializeTester = new AvroStockCompilerSerializeTester
val avroScalaCompSerializeTester = new AvroScalaCompilerSerializeTester
val javaSerializeTester          = new JavaSerializeTester

// Warm up the JVM so that the JIT kicks in
avroStockCompSerializeTester.runTest(100000)
avroScalaCompSerializeTester.runTest(100000)
javaSerializeTester.runTest(100000)

println("Message sizes: ")
println("Stock: " + avroStockCompSerializeTester.messageSize)
println("Scala: " + avroScalaCompSerializeTester.messageSize)
println("Java:  " + javaSerializeTester.messageSize)

// Serialization testing
// write 1 million new instances of the records to byte arrays. do this 10
// times for each

(1 to 5).foreach( i => {
    println("Test run: " + i)
    val stockTimes = avroStockCompSerializeTester.runTest(1000000)
    println("avroStockTime (serial, deserial): " + stockTimes)
    val scalaTimes = avroScalaCompSerializeTester.runTest(1000000)
    println("avroScalaTime (serial, deserial): " + scalaTimes)
    val javaTimes = javaSerializeTester.runTest(1000000)
    println("javaTime      (serial, deserial): " + javaTimes)
})

