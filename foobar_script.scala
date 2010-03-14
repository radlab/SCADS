import a.b.c._

import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer

import org.apache.avro.specific._
import org.apache.avro.io._

val fbz = FooBar(15, false, "abc", Array[Byte](55,66) )

val writer = new SpecificDatumWriter[FooBar](fbz.getSchema)
val buffer = new ByteArrayOutputStream(128)
val encoder = new BinaryEncoder(buffer)
writer.write(fbz, encoder) 
val data: Array[Byte] = buffer.toByteArray


val reader = new SpecificDatumReader[FooBar](fbz.getSchema)
val decoderFactory = new DecoderFactory
val inStream = decoderFactory.createBinaryDecoder(data, null)
val fbz2 = FooBar(0, false, "", Array[Byte]())
reader.read(fbz2, inStream)

println("fbz:  " + fbz)
println("fbz2: " + fbz2)
