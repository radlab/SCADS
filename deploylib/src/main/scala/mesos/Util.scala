package deploylib
package mesos

import java.io.{ File, InputStream, BufferedReader, InputStreamReader, FileOutputStream, ByteArrayOutputStream }
import org.apache.avro.generic.{ IndexedRecord, GenericData, GenericRecord, GenericDatumReader, GenericDatumWriter }
import org.apache.avro.io.{
  BinaryData,
  DecoderFactory,
  BinaryEncoder,
  BinaryDecoder,
  DatumReader,
  DatumWriter,
  ResolvingDecoder
}
import org.apache.avro.specific.{ SpecificRecord, SpecificDatumReader, SpecificDatumWriter }
import edu.berkeley.cs.scads.comm._
import edu.berkeley.cs.avro.runtime._

object JvmTask {
  val schema = TypedSchemas.schemaOf[JvmTask]
  val decoderFactory: DecoderFactory = (new DecoderFactory).configureDirectDecoder(true)
  val reader = new SpecificDatumReader[JvmTask](schema)
  val writer = new SpecificDatumWriter[JvmTask](schema)

  def apply(bytes: Array[Byte]): JvmTask = {
    val dec = decoderFactory.createBinaryDecoder(bytes, null)
    reader.read(null, dec)
  }

  def apply(task: JvmTask): Array[Byte] = {
    val out = new ByteArrayOutputStream(1024)
    val binEncoder  = new BinaryEncoder(out)
    writer.write(task, binEncoder)
    out.toByteArray
  }
}

class StreamTailer(stream: InputStream, size: Int = 100) extends Runnable {
  val reader = new BufferedReader(new InputStreamReader(stream))
  val thread = new Thread(this, "StreamEchoer")
  var lines = new Array[String](size)
  var pos = 0
  thread.start()

  def run() = {
    var line = reader.readLine()
    while (line != null) {
      println(line)
      lines(pos) = line
      pos = (pos + 1) % size
      line = reader.readLine()
    }
  }

  def tail: String = {
    val startPos = pos
    (0 to size).flatMap(i => Option(lines((startPos + i) % size))).mkString("\n")
  }
}
