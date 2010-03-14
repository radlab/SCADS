import edu.berkeley.cs.scads.comm._

import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer

import org.apache.avro.specific._
import org.apache.avro.io._

val storageRequest = StorageRequest( 123L,
    GetRequest(
        "my_namespace",
        "my_key".getBytes))

val writer = new SpecificDatumWriter[StorageRequest](storageRequest.getSchema)
val buffer = new ByteArrayOutputStream(128)
val encoder = new BinaryEncoder(buffer)
writer.write(storageRequest, encoder) 
val data: Array[Byte] = buffer.toByteArray


val reader = new SpecificDatumReader[StorageRequest](storageRequest.getSchema)
val decoderFactory = new DecoderFactory
val inStream = decoderFactory.createBinaryDecoder(data, null)
val storageRequest2 = new StorageRequest
reader.read(storageRequest2, inStream)

storageRequest2.body match {
    case GetRequest(ns, key) => println("NS: " + ns + ", KEY: " + new String(key))
    case _ => println("oops, wrong req type")
}
