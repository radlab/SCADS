package a.b.c

import com.googlecode.avro.annotation.AvroRecord

@AvroRecord
case class FooBar(var x: Int, var y: Boolean, var z: String, var b: Array[Byte])

object Baz {
    val findMe = "abc"
}

@AvroRecord
case class Baz(var x: Int, var y: Double)

case class IgnoreMe(var y: String)
