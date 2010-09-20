package benchmarks
package plugin

import com.googlecode.avro.marker._

case class Primitives(var i: Int, var j: Long, var k: Boolean) extends AvroRecord

case class SeqOfPrimitives(var i: Seq[Int], var j: Seq[Long]) extends AvroRecord

case class ByteContainer(var i: Array[Byte]) extends AvroRecord

case class MessageContainer(var i: InnerMessage1, var j: InnerMessage2) extends AvroRecord

case class InnerMessage1(var i: Float) extends AvroRecord

case class InnerMessage2(var i: Double) extends AvroRecord

case class RecordList(var i: List[StringRec]) extends AvroRecord

case class StringRec(var i: org.apache.avro.util.Utf8) extends AvroRecord
