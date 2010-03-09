import a.b.c._

import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer

import org.apache.avro.specific._
import org.apache.avro.io._

val baz = Baz(15, 33.503)

val writer = new SpecificDatumWriter[Baz](baz.getSchema)
val buffer = new ByteArrayOutputStream(128)
val encoder = new BinaryEncoder(buffer)
writer.write(baz, encoder) 
val data: Array[Byte] = buffer.toByteArray


val reader = new SpecificDatumReader[Baz](baz.getSchema)
val decoderFactory = new DecoderFactory
val inStream = decoderFactory.createBinaryDecoder(data, null)
val baz2 = Baz(0, 0.0)
reader.read(baz2, inStream)

println("baz:  " + baz)
println("baz2: " + baz2)
