package a.b.c

import com.googlecode.avro.annotation.AvroRecord

@com.googlecode.avro.annotation.AvroRecord
case class FooBar(var x: Int, var y: Boolean, var z: String, var b: Array[Byte]) {
    //def this() = this(0,false,null,null)
}

@com.googlecode.avro.annotation.AvroRecord
case class Baz(var x: Int, var y: Double)

case class IgnoreMe(var y: String) {
    
    def mkByteBuffer(bytes: Array[Byte]) = java.nio.ByteBuffer.wrap(bytes)

}
