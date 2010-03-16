import edu.berkeley.cs.scads.comm._

import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer

import org.apache.avro.specific._
import org.apache.avro.io._

import scala.reflect.Manifest

def toByteArray[ T <: SpecificRecord ](request: T) = {
    val writer = new SpecificDatumWriter[T](request.getSchema)
    val buffer = new ByteArrayOutputStream(128)
    val encoder = new BinaryEncoder(buffer)
    writer.write(request, encoder) 
    buffer.toByteArray
}

def fromByteArray[ T <: SpecificRecord ](bytes: Array[Byte])(implicit manifest: Manifest[T]):T = {
    val newInstance = manifest.erasure.newInstance.asInstanceOf[T]
    val reader = new SpecificDatumReader[T](newInstance.getSchema)
    val decoderFactory = new DecoderFactory
    val inStream = decoderFactory.createBinaryDecoder(bytes, null)
    reader.read(newInstance, inStream)
    newInstance
}

val storageRequest = StorageRequest( 123L,
    GetRequest(
        "my_namespace",
        "my_key".getBytes))

val storageRequest2 = fromByteArray[ StorageRequest ]( toByteArray (storageRequest ) )

storageRequest2.body match {
    case GetRequest(ns, key) => println("NS: " + ns + ", KEY: " + new String(key))
    case _ => println("oops, wrong req type")
}

val storageResponse = StorageResponse( 456L, 
        RecordSet(
            List(
                Record( "myKey1".getBytes, "myValue1".getBytes ),
                Record( "myKey2".getBytes, "myValue2".getBytes ))) )

val storageResponse2 = fromByteArray[ StorageResponse ]( toByteArray (storageResponse ) )

storageResponse2.body match {
    case RecordSet(records) => println("records: " + records) 
    case _ => println("oops, fail")
}

val storageRequest3 = StorageRequest( 678L,
    ConfigureRequest("myNamespace", "myPartition"))

val storageRequest4 = fromByteArray[ StorageRequest ]( toByteArray (storageRequest3) )

storageRequest4.body match {
    case ConfigureRequest( ns, partition ) => println("ConfigureRequest: " + ns + ", " + partition)
    case _ => println("oops fail")
}

