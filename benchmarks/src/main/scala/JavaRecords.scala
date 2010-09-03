package benchmarks
package javarec

case class Primitives(i: Int, j: Long, k: Boolean)

case class SeqOfPrimitives(i: Seq[Int], j: Seq[Long])

case class ByteContainer(i: Array[Byte])

case class MessageContainer(i: InnerMessage1, j: InnerMessage2)

case class InnerMessage1(i: Float)

case class InnerMessage2(i: Double)

case class RecordList(i: List[StringRec])

case class StringRec(i: String)
