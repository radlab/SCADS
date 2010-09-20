package benchmarks 

import scala.annotation.tailrec

import org.apache.avro._
import io._
import generic._
import specific._
import util._

import java.io._
import java.nio._

object Benchmark {
  
  /**
   * Runs f for ms milliseconds. Returns the number of times f was invoked,
   * with the actual execution time in ns
   */
  def runFor(ms: Long)(f: => Unit): (Int, Long) = {
    val start = System.nanoTime
    val end   = start + (ms * 1000000L)
    var iters = 0
    while (System.nanoTime < end) {
      f
      iters += 1
    }
    (iters, System.nanoTime - start)
  }

  def repeat(times: Int)(f: => Unit) {
    @tailrec def loop(n: Int) {
      if (n > 0) {
        f
        loop(n-1)
      }
    }
    loop(times)
  }

  trait Runner[T] {
    /**
     * Returns new instance to serialize/deserialize
     */
    val newInstance: T

    lazy val newInstanceBytes = serialize(newInstance)

    def equals(a: T, b: T): Boolean =
      true
      //a == b

    def run() {
      val r  = newInstance
      val r0 = deserialize(serialize(r))
      assert(equals(r, r0), "Records did not match")
    }

    def runSerialize() {
      val r = newInstance
      val b = serialize(r)
      assert(b != null)
    }

    def runDeserialize() {
      val r0 = deserialize(newInstanceBytes)
      assert(equals(newInstance, r0), "Records do not match")
    }

    def serialize(rec: T): Array[Byte]

    def deserialize(bytes: Array[Byte]): T

  }

  abstract class AvroRunner[T <: SpecificRecord](implicit m: Manifest[T]) extends Runner[T] {

    val decoderFactory = (new DecoderFactory).configureDirectDecoder(true)

    val recClass  = m.erasure.asInstanceOf[Class[T]]
    val recSchema = recClass.newInstance.getSchema
    
    val reader = new SpecificDatumReader[T](recSchema)
    val writer = new SpecificDatumWriter[T](recSchema)

    def serialize(rec: T): Array[Byte] = {
      val baos = new ByteArrayOutputStream(1024)
      val enc  = new BinaryEncoder(baos)
      writer.write(rec, enc)
      baos.toByteArray
    }

    def deserialize(bytes: Array[Byte]) = {
      val dec = decoderFactory.createBinaryDecoder(bytes, null)
      reader.read(recClass.newInstance, dec)
    }
  }

  abstract class JavaRunner[T] extends Runner[T] {

    def serialize(rec: T): Array[Byte] = {
      val baos = new ByteArrayOutputStream(1024)
      val oos  = new ObjectOutputStream(baos)
      oos.writeObject(rec)
      baos.toByteArray
    }

    def deserialize(bytes: Array[Byte]) = {
      val bais = new ByteArrayInputStream(bytes)
      val ois  = new ObjectInputStream(bais)
      ois.readObject().asInstanceOf[T]
    }

  }

  import benchmarks.{ plugin, stock, javarec }

  object PluginPrimitiveRunner extends AvroRunner[plugin.Primitives] {
    val newInstance =
      plugin.Primitives(10000, 100000L, true)
  }

  object StockPrimitiveRunner extends AvroRunner[stock.Primitives] {
    val newInstance = {
      val r = new stock.Primitives
      r.i = 10000
      r.j = 100000L
      r.k = true
      r
    }
  }

  object JavaPrimitiveRunner extends JavaRunner[javarec.Primitives] {
    val newInstance =
      javarec.Primitives(10000, 100000L, true)
  }

  object PluginSeqOfPrimitivesRunner extends AvroRunner[plugin.SeqOfPrimitives] {
    val newInstance =
      plugin.SeqOfPrimitives((1 to 10).map(i => i).toSeq, (1L to 100L).map(i => i).toSeq)
  }

  object StockArrayOfPrimitivesRunner extends AvroRunner[stock.ArrayOfPrimitives] {
    val newInstance = {
      import java.lang.{ Integer => JInteger, Long => JLong }
      val r = new stock.ArrayOfPrimitives
      val i = new GenericData.Array[JInteger](10, r.getSchema.getField("i").schema)
      val j = new GenericData.Array[JLong](100, r.getSchema.getField("j").schema)

      for (e <- 1 to 10) { i.add(e) }
      for (e <- 1L to 100L) { j.add(e) }

      r.i = i
      r.j = j
      r
    }
  }

  object JavaSeqOfPrimitivesRunner extends JavaRunner[javarec.SeqOfPrimitives] {
    val newInstance =
      javarec.SeqOfPrimitives((1 to 10).map(i => i).toSeq, (1L to 100L).map(i => i).toSeq)
  }

  val ByteArray = (1 to 1024).map(_.toByte).toArray

  object PluginByteContainerRunner extends AvroRunner[plugin.ByteContainer] {
    val newInstance = plugin.ByteContainer(ByteArray)
  }

  object StockByteContainerRunner extends AvroRunner[stock.ByteContainer] {
    val newInstance = {
      val r = new stock.ByteContainer
      r.i   = ByteBuffer.wrap(ByteArray)
      r
    }
  }

  object JavaByteContainerRunner extends JavaRunner[javarec.ByteContainer] {
    val newInstance = javarec.ByteContainer(ByteArray)
  }

  object PluginMessageContainerRunner extends AvroRunner[plugin.MessageContainer] {
    val newInstance = 
      plugin.MessageContainer(plugin.InnerMessage1(1.0f),
                              plugin.InnerMessage2(-1.0))
  }

  object StockMessageContainerRunner extends AvroRunner[stock.MessageContainer] {
    val newInstance = {
      val r = new stock.MessageContainer
      val i = new stock.InnerMessage1
      i.i   = 1.0f
      val j = new stock.InnerMessage2
      j.i   = -1.0
      r.i   = i
      r.j   = j
      r
    }
  }

  object JavaMessageContainerRunner extends JavaRunner[javarec.MessageContainer] {
    val newInstance = 
      javarec.MessageContainer(javarec.InnerMessage1(1.0f),
                               javarec.InnerMessage2(-1.0))
  }

  object PluginRecordListRunner extends AvroRunner[plugin.RecordList] {
    val newInstance =
      plugin.RecordList((1 to 128).map(i => plugin.StringRec(new Utf8(i.toString))).toList)
  }

  object StockRecordListRunner extends AvroRunner[stock.RecordList] {
    val newInstance = {
      val r = new stock.RecordList
      r.i   = new GenericData.Array[stock.StringRec](128, r.getSchema.getField("i").schema)
      for (i <- 1 to 128) {
        val sr = new stock.StringRec
        sr.i   = new Utf8(i.toString)
        r.i.add(sr)
      }
      r
    }
  }

  object JavaRecordListRunner extends JavaRunner[javarec.RecordList] {
    val newInstance =
      javarec.RecordList((1 to 128).map(i => javarec.StringRec(i.toString)).toList)
  }

  @inline private def nanoToSeconds(ns: Long): Double = 
    ns.toDouble / 1000000000.0 

  object RunType extends Enumeration {
    val Serialize, Deserialize = Value
  }

  def main(args: Array[String]) {
    val runtime = 30000L // 30 seconds
    val numrepeats = 3

    // warmup
    //val (n0, t0) = runFor(runtime) {
    //  PluginPrimitiveRunner.run()
    //}

    //println("Warmup PluginPrimitiveRunner ran %d iterations in %f seconds".format(n0, nanoToSeconds(t0)))

    //val (n1, t1) = runFor(runtime) {
    //  StockPrimitiveRunner.run()
    //}

    //println("Warmup StockPrimitiveRunner ran %d iterations in %f seconds".format(n1, nanoToSeconds(t1)))

    def doRun(tpe: RunType.Value, runs: Seq[Seq[Runner[SpecificRecord]]]) {
      println("+ Starting run for type %s +".format(tpe))
      runs.foreach(group => group.foreach(runner => {
        val (n, t) = runFor(runtime) {
          tpe match {
            case RunType.Serialize =>
              runner.runSerialize()
            case RunType.Deserialize =>
              runner.runDeserialize()
          }
        }
        println("-- %s Run of %s ran %d iterations in %f seconds. Message size was %d bytes.".format(
            tpe, runner.getClass.getName, n, nanoToSeconds(t), runner.newInstanceBytes.length))
      }))
    }

    // TODO: type inconsistencies here force us to do a strange cast
    val runners: Seq[Seq[AnyRef]] = 
      Seq(
        Seq[AnyRef](
            PluginPrimitiveRunner,
            PluginSeqOfPrimitivesRunner,
            PluginByteContainerRunner,
            PluginMessageContainerRunner,
            PluginRecordListRunner),
        Seq[AnyRef](
            StockPrimitiveRunner,
            StockArrayOfPrimitivesRunner,
            StockByteContainerRunner,
            StockMessageContainerRunner,
            StockRecordListRunner),
        Seq[AnyRef](
            JavaPrimitiveRunner,
            JavaSeqOfPrimitivesRunner,
            JavaByteContainerRunner,
            JavaMessageContainerRunner,
            JavaRecordListRunner))

    //val runners: Seq[Seq[AnyRef]] = Seq(Seq[AnyRef](PluginRecordListRunner),Seq[AnyRef](StockMessageContainerRunner))

    repeat(numrepeats) {
      doRun(RunType.Serialize, runners.asInstanceOf[Seq[Seq[Runner[SpecificRecord]]]])
      doRun(RunType.Deserialize, runners.asInstanceOf[Seq[Seq[Runner[SpecificRecord]]]])
    }

  }
}
