package edu.berkeley.cs.scads.test

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, Spec}
import org.scalatest.matchers.ShouldMatchers

import edu.berkeley.cs.scads.comm._
import edu.berkeley.cs.scads.comm.Conversions._
import edu.berkeley.cs.scads.storage._
import edu.berkeley.cs.avro.marker.AvroRecord
import org.apache.avro.generic._
import org.apache.avro.util.Utf8
import net.lag.logging.Logger
import collection.mutable.HashSet
import edu.berkeley.cs.avro.runtime.ScalaSpecificRecord

@RunWith(classOf[JUnitRunner])
class HashKeyValueStoreSpec extends AbstracKeyValueStoreSpec {
  override def createNamespace[KeyType <: ScalaSpecificRecord,
        ValueType <: ScalaSpecificRecord]
     (ns: String)
     (implicit keyType: scala.reflect.Manifest[KeyType], valueType: scala.reflect.Manifest[ValueType])
           : KeyValueStore[KeyType, ValueType, ValueType, (KeyType, ValueType)]
                with QuorumProtocol[KeyType, ValueType, ValueType, (KeyType, ValueType)]
                with SpecificNamespaceTrait[KeyType, ValueType] = {
    cluster.getHashNamespace[KeyType, ValueType](ns, 0 :: Nil)
  }
}

@RunWith(classOf[JUnitRunner])
class RangeKeyValueStoreSpec extends AbstracKeyValueStoreSpec {
  override def createNamespace[KeyType <: ScalaSpecificRecord,
        ValueType <: ScalaSpecificRecord]
     (ns: String)
     (implicit keyType: scala.reflect.Manifest[KeyType], valueType: scala.reflect.Manifest[ValueType])
           : KeyValueStore[KeyType, ValueType, ValueType, (KeyType, ValueType)]
                with QuorumProtocol[KeyType, ValueType, ValueType, (KeyType, ValueType)]
                with SpecificNamespaceTrait[KeyType, ValueType] = {
    cluster.getNamespace[KeyType, ValueType](ns)
  }

  describe("getRange Method") {
    it("should have bulkGetRange") {
      //val ns = createNamespace[IntRec, IntRec]("bulkrangetest")

      ///* Insert Integers 1-100 */
      //(1 to 100).foreach(i => ns.put(IntRec(i),IntRec(i)))

      //val queriesAnswers =
      //  ((null, null), (1 to 100)) ::
      //  ((null, IntRec(50)), (1 until 50)) ::
      //  ((IntRec(50), null), (50 to 100)) ::
      //  ((IntRec(10), IntRec(90)), (10 until 90)) ::
      //  ((IntRec(-10), null), (1 to 100)) ::
      //  ((null, IntRec(110)), (1 to 100)) ::
      //  ((IntRec(-10), IntRec(110)), (1 to 100)) :: Nil

      //val futures = ns.asyncGetRange(queriesAnswers.map(_._1).map(q => (Option(q._1), Option(q._2))))
      //  futures.zip(queriesAnswers.map(_._2)).foreach {
      //  case (future, correctAnswer) => future().map(_._1.f1) should equal(correctAnswer)
      //}

      pending
    }


    it("should correctly return a count of records") {pending}

    it("should suport prefixKeys") {
      val ns = createNamespace[IntRec3, IntRec]("prefixrange")
      val data = for (i <- 1 to 10; j <- 1 to 10; k <- 1 to 10)
      yield (IntRec3(i, k, j), IntRec(1))

      ns ++= data

      def prefixRec(i: Option[Int], j: Option[Int], k: Option[Int]): GenericData.Record = {
        val rec = new GenericData.Record(IntRec3.schema)
        rec.put(0, i.orNull: Any)
        rec.put(1, j.orNull: Any)
        rec.put(2, k.orNull: Any)
        rec
      }

      ns.genericNamespace.getRange(prefixRec(1, None, None), prefixRec(1, None, None)).size should equal(100)
      ns.genericNamespace.getRange(prefixRec(1, None, None), prefixRec(2, None, None)).size should equal(200)
      ns.genericNamespace.getRange(prefixRec(1, None, None), prefixRec(3, None, None)).size should equal(300)
      ns.genericNamespace.getRange(prefixRec(2, None, None), prefixRec(3, None, None)).size should equal(200)
      ns.genericNamespace.getRange(prefixRec(3, None, None), prefixRec(3, None, None)).size should equal(100)

      ns.genericNamespace.getRange(prefixRec(1, None, None), prefixRec(1, None, None), ascending = false).size should equal(100)
      ns.genericNamespace.getRange(prefixRec(1, None, None), prefixRec(2, None, None), ascending = false).size should equal(200)
      ns.genericNamespace.getRange(prefixRec(1, None, None), prefixRec(3, None, None), ascending = false).size should equal(300)
      ns.genericNamespace.getRange(prefixRec(2, None, None), prefixRec(3, None, None), ascending = false).size should equal(200)
      ns.genericNamespace.getRange(prefixRec(3, None, None), prefixRec(3, None, None), ascending = false).size should equal(100)
    }

    it("should suport prefixKeys with strings") {
      val ns = createNamespace[StringRec3, IntRec]("prefixrangestring")
      def toString(i: Int) = "%04d".format(i)
      def toUtf8(i: Int) = new Utf8("%04d".format(i))
      val data = for (i <- 1 to 10; j <- 1 to 10; k <- 1 to 10)
      yield (StringRec3(toString(i), toString(k), toString(j)), IntRec(1))

      ns ++= data

      def prefixRec(i: Option[Int], j: Option[Int], k: Option[Int]): GenericData.Record = {
        val rec = new GenericData.Record(StringRec3.schema)
        rec.put(0, i.map(toUtf8).orNull: Any)
        rec.put(1, j.map(toUtf8).orNull: Any)
        rec.put(2, k.map(toUtf8).orNull: Any)
        rec
      }

      ns.genericNamespace.getRange(prefixRec(1, None, None), prefixRec(1, None, None)).size should equal(100)
      ns.genericNamespace.getRange(prefixRec(1, None, None), prefixRec(2, None, None)).size should equal(200)
      ns.genericNamespace.getRange(prefixRec(1, None, None), prefixRec(3, None, None)).size should equal(300)
      ns.genericNamespace.getRange(prefixRec(2, None, None), prefixRec(3, None, None)).size should equal(200)
      ns.genericNamespace.getRange(prefixRec(3, None, None), prefixRec(3, None, None)).size should equal(100)

      ns.genericNamespace.getRange(prefixRec(1, None, None), prefixRec(1, None, None), ascending = false).size should equal(100)
      ns.genericNamespace.getRange(prefixRec(1, None, None), prefixRec(2, None, None), ascending = false).size should equal(200)
      ns.genericNamespace.getRange(prefixRec(1, None, None), prefixRec(3, None, None), ascending = false).size should equal(300)
      ns.genericNamespace.getRange(prefixRec(2, None, None), prefixRec(3, None, None), ascending = false).size should equal(200)
      ns.genericNamespace.getRange(prefixRec(3, None, None), prefixRec(3, None, None), ascending = false).size should equal(100)
    }

    it("should support composite primary keys") {
      /* simple get/puts */
      val ns = createNamespace[CompIntStringRec, IntRec]("compkeytest")

      val data = for (i <- 1 to 10; c <- 'a' to 'c')
      yield (CompIntStringRec(IntRec(i), StringRec(c.toString)), IntRec(1))

      data.foreach(kv => ns.put(kv._1, kv._2))
      data.foreach(kv => ns.get(kv._1) should equal(Some(kv._2)))

      /* getRange, full records */
      ns.getRange(None, None) should equal(data)

      /* getRange, prefix */
      ns.getRange(Some(CompIntStringRec(IntRec(1), null)), Some(CompIntStringRec(IntRec(1), null))) should equal(
        ('a' to 'c').map(c => (CompIntStringRec(IntRec(1), StringRec(c.toString)), IntRec(1))))

    }

    it("should correctly return ranges") {
      val ns = createNamespace[IntRec, IntRec]("rangetest")

      /* Insert Integers 1-100 */
      (1 to 100).foreach(i => ns.put(IntRec(i), IntRec(i)))

      /* Various range checks */
      ns.getRange(None, None).map(_._1.f1) should equal(1 to 100)
      ns.getRange(None, IntRec(50)).map(_._1.f1) should equal(1 until 50)
      ns.getRange(IntRec(50), None).map(_._1.f1) should equal(50 to 100)
      ns.getRange(IntRec(10), IntRec(90)).map(_._1.f1) should equal(10 until 90)
      ns.getRange(IntRec(-10), None).map(_._1.f1) should equal(1 to 100)
      ns.getRange(None, IntRec(110)).map(_._1.f1) should equal(1 to 100)
      ns.getRange(IntRec(-10), IntRec(110)).map(_._1.f1) should equal(1 to 100)
    }

    it("should correctly return ranges obeying the limit") {
      val ns = createNamespace[IntRec, IntRec]("rangetestlimit")

      /* Insert Integers 1-100 */
      (1 to 100).foreach(i => ns.put(IntRec(i), IntRec(i)))

      /* Various range checks */
      ns.getRange(None, None, limit = 10).map(_._1.f1) should equal(1 to 10)
      ns.getRange(None, IntRec(50), limit = 10).map(_._1.f1) should equal(1 to 10)
      ns.getRange(IntRec(50), None, limit = 10).map(_._1.f1) should equal(50 until 60)
      ns.getRange(IntRec(10), IntRec(90), limit = 10).map(_._1.f1) should equal(10 until 20)
      ns.getRange(IntRec(-10), None, limit = 10).map(_._1.f1) should equal(1 to 10)
      ns.getRange(None, IntRec(110), limit = 10).map(_._1.f1) should equal(1 to 10)
      ns.getRange(IntRec(-10), IntRec(110), limit = 10).map(_._1.f1) should equal(1 to 10)
    }

    it("should correctly return ranges backwards") {
      val ns = createNamespace[IntRec, IntRec]("rangetestbackwards")

      /* Insert Integers 1-100 */
      (1 to 100).foreach(i => ns.put(IntRec(i), IntRec(i)))

      /* Various range checks */
      ns.getRange(None, None, ascending = false).map(_._1.f1) should equal(100 to 1 by -1)
      ns.getRange(None, IntRec(50), ascending = false).map(_._1.f1) should equal(49 to 1 by -1)
      ns.getRange(IntRec(50), None, ascending = false).map(_._1.f1) should equal(100 to 50 by -1)
      ns.getRange(IntRec(10), IntRec(90), ascending = false).map(_._1.f1) should equal(89 to 10 by -1)
      ns.getRange(IntRec(-10), None, ascending = false).map(_._1.f1) should equal(100 to 1 by -1)
      ns.getRange(None, IntRec(110), ascending = false).map(_._1.f1) should equal(100 to 1 by -1)
      ns.getRange(IntRec(-10), IntRec(110), ascending = false).map(_._1.f1) should equal(100 to 1 by -1)

    }

    it("should correctly return ranges backwards with limit") {
      val ns = createNamespace[IntRec, IntRec]("rangetestlimitbackwards")
      /* Insert Integers 1-100 */
      (1 to 100).foreach(i => ns.put(IntRec(i), IntRec(i)))

      ns.getRange(None, None, limit = 10, ascending = false).map(_._1.f1) should equal(100 to 91 by -1)
      ns.getRange(None, IntRec(50), limit = 10, ascending = false).map(_._1.f1) should equal(49 to 40 by -1)
      ns.getRange(IntRec(50), None, limit = 10, ascending = false).map(_._1.f1) should equal(100 to 91 by -1)
      ns.getRange(IntRec(10), IntRec(90), limit = 10, ascending = false).map(_._1.f1) should equal(89 to 80 by -1)
      ns.getRange(IntRec(-10), None, limit = 10, ascending = false).map(_._1.f1) should equal(100 to 91 by -1)
      ns.getRange(None, IntRec(110), limit = 10, ascending = false).map(_._1.f1) should equal(100 to 91 by -1)
      ns.getRange(IntRec(-10), IntRec(110), limit = 10, ascending = false).map(_._1.f1) should equal(100 to 91 by -1)

    }


    it("should implement asyncGetRange") {

      val ns = createNamespace[IntRec, IntRec]("asyncgetrangetest")

      /* Insert Integers 1-100 */
      (1 to 100).foreach(i => ns.put(IntRec(i), IntRec(i)))

      /* Various range checks */
      ns.asyncGetRange(None, None).get.map(_._1.f1) should equal(1 to 100)
      ns.asyncGetRange(None, IntRec(50)).get.map(_._1.f1) should equal(1 until 50)
      ns.asyncGetRange(IntRec(50), None).get.map(_._1.f1) should equal(50 to 100)
      ns.asyncGetRange(IntRec(10), IntRec(90)).get.map(_._1.f1) should equal(10 until 90)
      ns.asyncGetRange(IntRec(-10), None).get.map(_._1.f1) should equal(1 to 100)
      ns.asyncGetRange(None, IntRec(110)).get.map(_._1.f1) should equal(1 to 100)
      ns.asyncGetRange(IntRec(-10), IntRec(110)).get.map(_._1.f1) should equal(1 to 100)

    }

    it("should delete data with range check") {
      val ns = createNamespace[IntRec, StringRec]("kvstore_deletetest_range")

      /* Insert Integers 1-100 */
      (1 to 100).foreach(i => ns.put(IntRec(i), StringRec(i.toString)))
      /* Delete all odd records */
      (1 to 100 by 2).foreach(i => ns.put(IntRec(i), None))
      /* Make sure even ones are still there and odd ones are gone */
      (1 to 100).foreach(i => ns.get(IntRec(i)) should equal(if(i % 2 == 0) Some(StringRec(i.toString)) else None))
      /* Make sure gaps aren't left when we getRange over deleted data */
      ns.getRange(None, None).map(_._1.f1) should equal(2 to 100 by 2)
    }
  }
}


abstract class AbstracKeyValueStoreSpec extends Spec with ShouldMatchers with BeforeAndAfterAll {
  val cluster = TestScalaEngine.newScadsCluster(5)
  val logger = Logger()

  override def afterAll(): Unit = {
    cluster.shutdownCluster()
  }

  def createNamespace[KeyType <: ScalaSpecificRecord,
        ValueType <: ScalaSpecificRecord]
     (ns: String)
     (implicit keyType: scala.reflect.Manifest[KeyType], valueType: scala.reflect.Manifest[ValueType])
           : KeyValueStore[KeyType, ValueType, ValueType, (KeyType, ValueType)]
                with QuorumProtocol[KeyType, ValueType, ValueType, (KeyType, ValueType)]
                with SpecificNamespaceTrait[KeyType, ValueType]

  val rand = new scala.util.Random(123456789)

  /**
   * Generates count random strings
   */
  def randomStrings(count: Int): Array[String] = {
    require(count >= 0)

    val strlen = 20

    val ret = new Array[String](count)
    val seenBefore = new HashSet[String]
    var pos = 0

    while (pos < count) {
      val chArray = new Array[Char](strlen)
      var i = 0
      while (i < strlen) {
        //chArray(i) = rand.nextInt(256).toChar
        chArray(i) = rand.nextPrintableChar() // easier to debug for now
        i += 1
      }
      val str = new String(chArray)
      if (!(seenBefore contains str)) {
        seenBefore += str
        ret(pos) = str
        pos += 1
      }
    }

    ret
  }


  var keys: Array[StringRec] = randomStrings(501).map(new StringRec(_))


  implicit def toOption[A](a: A): Option[A] = Option(a)

  describe("SCADS Namespace") {
    it("should implement get/put") {
      val ns = createNamespace[StringRec, StringRec]("getputtest")

      (1 to 100).foreach(i => ns.put(keys(i), StringRec(i.toString)))
      (1 to 100).foreach(i => ns.get(keys(i)) should equal(Some(StringRec(i.toString))))
    }

    it("should block the quorum for a put") {
      val ns = createNamespace[StringRec, StringRec]("quorumtest")
      for (i <- 0 to 100) {
        ns.put(keys(1), StringRec("string1"))
        ns.get(keys(1)) should equal(Some(StringRec("string1")))
        ns.put(keys(1), StringRec("string2"))
        ns.get(keys(1)) should equal(Some(StringRec("string2")))
      }
    }

    it("should delete data") {
      val ns = createNamespace[StringRec, StringRec]("kvstore_deletetest")

      /* Insert Integers 1-100 */
      (1 to 100).foreach(i => ns.put(keys(i), StringRec(i.toString)))
      /* Delete all odd records */
      (1 to 100 by 2).foreach(i => ns.put(keys(i), None))
      /* Make sure even ones are still there and odd ones are gone */
      (1 to 100).foreach(i => ns.get(keys(i)) should equal(if (i % 2 == 0) Some(StringRec(i.toString)) else None))
    }

    it("should implement asyncGet") {
      val ns = createNamespace[StringRec, IntRec]("asyncgettest")

      val recs = (1 to 500).map(i => (keys(i), IntRec(3 * i)))
      ns ++= recs

      val ftchs = (1 to 500).map(i => ns.asyncGet(keys(i)))

      ftchs.zip((1 to 500)).foreach {
        case (ftch, i) =>
          ftch() should equal(Some(IntRec(3 * i)))
      }
    }





    it("should return all versions") {
      val ns = createNamespace[IntRec, StringRec]("allversions")
      ns.put(IntRec(1), StringRec("string1"))
      val values = ns.getAllVersions(IntRec(1)).map(_._2.get.f1)
      values should contain("string1")
    }

    it("should implement test/set") {pending}


    it("should handle prefix schema resolution automatically") {
      import scala.collection.JavaConversions._

      import org.apache.avro.{generic, Schema}
      import generic._
      import Schema._

      import org.codehaus.jackson.node.NullNode

      // 1 int field
      val oldSchema =
      Schema.createRecord("TestRec", "", "testns", false)
      oldSchema.setFields(Seq(new Field("f1", Schema.create(Type.INT), "", null)))

      // 2 int fields, f2 optional
      val newSchema =
      Schema.createRecord("TestRec", "", "testns", false)
      newSchema.setFields(Seq(new Field("f1", Schema.create(Type.INT), "", null),
        new Field("f2", Schema.createUnion(Seq(Schema.create(Type.NULL), Schema.create(Type.INT))), "", NullNode.instance)))

      // load the cluster once with an "old" version (1 field)
      val oldNs = cluster.getNamespace("prefixSchemaRes", oldSchema, oldSchema)

      // now, load the cluster with a "new" version (2 fields)
      val newNs = cluster.getNamespace("prefixSchemaRes", newSchema, newSchema)

      def newIntRec(f1: Int, f2: Option[Int]) = {
        val rec = new GenericData.Record(newSchema)
        rec.put(0, f1)
        rec.put(1, f2.map(i => java.lang.Integer.valueOf(i)).orNull)
        rec
      }

      for (i <- 1 to 10) {
        newNs.put(newIntRec(i, Some(i)), newIntRec(i, Some(i)))
        newNs.get(newIntRec(i, Some(i))) should equal(Some(newIntRec(i, None)))
      }

    }

    it("should handle general schema resolution automatically") {pending}

    it("should correctly return records by prefix") {pending}
    it("should allow data to be moved/copied") {pending}

    it("should efficently bulk load from iterator using ++=") {

      val ns = createNamespace[IntRec, StringRec]("pluspluseqtest")

      val recs = (1 to 5000).map(i => (IntRec(i), StringRec(i.toString)))
      ns ++= recs
      ns.getRange(None, None) should equal(recs)

      val recs2 = (10000 to 10100).map(i => (IntRec(i), StringRec(i.toString)))
      ns ++= recs2
      ns.getRange(Some(IntRec(10000)), None) should equal(recs2)
    }

    it("should allow you to truncate the contents of a namespace") {pending}
    it("should automatically handle schema resolution") {pending}

    it("should implement closure-shipping version of flatMap") {pending}

  }
}