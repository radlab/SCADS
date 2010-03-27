package localhost.test

import com.googlecode.avro.annotation.AvroRecord

object RecordASC
@AvroRecord case class RecordASC (var x: Int, var y: String, var z: Boolean)

case class RecordJS (var x: Int, var y: String, var z: Boolean) extends java.io.Serializable
