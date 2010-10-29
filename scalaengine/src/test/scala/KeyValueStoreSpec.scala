package edu.berkeley.cs.scads.test

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.Spec
import org.scalatest.matchers.ShouldMatchers

import edu.berkeley.cs.scads.comm._
import edu.berkeley.cs.scads.comm.Conversions._
import edu.berkeley.cs.scads.storage._
import edu.berkeley.cs.avro.marker.AvroRecord
import org.apache.avro.generic._
import org.apache.avro.util.Utf8
import net.lag.logging.Logger

@RunWith(classOf[JUnitRunner])
class KeyValueStoreSpec extends Spec with ShouldMatchers {
  val storageHandlers = TestScalaEngine.getTestHandler(5)
  val cluster = TestScalaEngine.getTestCluster()
  val logger = Logger()

  implicit def toOption[A](a: A): Option[A] = Option(a)

  describe("SCADS Namespace") {
    it("should implement get/put") {
      val ns = cluster.getNamespace[IntRec, StringRec]("getputtest")

      (1 to 100).foreach(i => ns.put(IntRec(i), StringRec(i.toString)))
      (1 to 100).foreach(i => ns.get(IntRec(i)) should equal(Some(StringRec(i.toString))))
    }

    it("should block the quorum for a put"){
      val ns = cluster.getNamespace[IntRec, StringRec]("quorumtest")
      for(i <- 0 to 100){
        ns.put(IntRec(1), StringRec("string1"))
        ns.get(IntRec(1)) should equal(Some(StringRec("string1")))
        ns.put(IntRec(1), StringRec("string2"))
        ns.get(IntRec(1)) should equal(Some(StringRec("string2")))
      }
    }

    it("should delete data") {
      val ns = cluster.getNamespace[IntRec, StringRec]("deletetest")

      /* Insert Integers 1-100 */
      (1 to 100).foreach(i => ns.put(IntRec(i), StringRec(i.toString)))
      /* Delete all odd records */
      (1 to 100 by 2).foreach(i => ns.put(IntRec(i), None))
      /* Make sure even ones are still there and odd ones are gone */
      (1 to 100).foreach(i => ns.get(IntRec(i)) should equal(if(i % 2 == 0) Some(StringRec(i.toString)) else None))
      /* Make sure gaps aren't left when we getRange over deleted data */
      ns.getRange(None, None).map(_._1.f1) should equal(2 to 100 by 2)
    }

    it("should implement asyncGet") {
      val ns = cluster.getNamespace[IntRec, IntRec]("asyncgettest")

      val recs = (1 to 500).map(i => (IntRec(i), IntRec(3 * i)))
      ns ++= recs

      val ftchs = (1 to 500).map(i => ns.asyncGet(IntRec(i)))

      ftchs.zip((1 to 500)).foreach {
        case (ftch, i) =>
          ftch() should equal(Some(IntRec(3 * i))) 
      }
    }

    describe("getRange Method") {
      it("should have bulkGetRange") {
        //val ns = cluster.getNamespace[IntRec, IntRec]("bulkrangetest")

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


      it("should suport prefixKeys") {
        val ns = cluster.getNamespace[IntRec3, IntRec]("prefixrange")
        val data = for(i <- 1 to 10; j <- 1 to 10; k <- 1 to 10)
          yield (IntRec3(i,k,j), IntRec(1))

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

        ns.genericNamespace.getRange(prefixRec(1, None, None), prefixRec(1, None, None), ascending=false).size should equal(100)
        ns.genericNamespace.getRange(prefixRec(1, None, None), prefixRec(2, None, None), ascending=false).size should equal(200)
        ns.genericNamespace.getRange(prefixRec(1, None, None), prefixRec(3, None, None), ascending=false).size should equal(300)
        ns.genericNamespace.getRange(prefixRec(2, None, None), prefixRec(3, None, None), ascending=false).size should equal(200)
        ns.genericNamespace.getRange(prefixRec(3, None, None), prefixRec(3, None, None), ascending=false).size should equal(100)
      }

      it("should suport prefixKeys with strings") {
        val ns = cluster.getNamespace[StringRec3, IntRec]("prefixrangestring")
        def toString(i: Int) = "%04d".format(i)
        def toUtf8(i: Int) = new Utf8("%04d".format(i))
        val data = for(i <- 1 to 10; j <- 1 to 10; k <- 1 to 10)
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

        ns.genericNamespace.getRange(prefixRec(1, None, None), prefixRec(1, None, None), ascending=false).size should equal(100)
        ns.genericNamespace.getRange(prefixRec(1, None, None), prefixRec(2, None, None), ascending=false).size should equal(200)
        ns.genericNamespace.getRange(prefixRec(1, None, None), prefixRec(3, None, None), ascending=false).size should equal(300)
        ns.genericNamespace.getRange(prefixRec(2, None, None), prefixRec(3, None, None), ascending=false).size should equal(200)
        ns.genericNamespace.getRange(prefixRec(3, None, None), prefixRec(3, None, None), ascending=false).size should equal(100)
      }

      it("should support composite primary keys") {
        /* simple get/puts */
        val ns = cluster.getNamespace[CompIntStringRec, IntRec]("compkeytest")
        
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
        val ns = cluster.getNamespace[IntRec, IntRec]("rangetest")

        /* Insert Integers 1-100 */
        (1 to 100).foreach(i => ns.put(IntRec(i),IntRec(i)))

        /* Various range checks */
        ns.getRange(None, None).map(_._1.f1)               should equal(1 to 100)
        ns.getRange(None, IntRec(50)).map(_._1.f1)         should equal(1 until 50)
        ns.getRange(IntRec(50), None).map(_._1.f1)         should equal(50 to 100)
        ns.getRange(IntRec(10), IntRec(90)).map(_._1.f1)   should equal(10 until 90)
        ns.getRange(IntRec(-10), None).map(_._1.f1)        should equal(1 to 100)
        ns.getRange(None, IntRec(110)).map(_._1.f1)        should equal(1 to 100)
        ns.getRange(IntRec(-10), IntRec(110)).map(_._1.f1) should equal(1 to 100)
      }

      it("should correctly return ranges obeying the limit") {
        val ns = cluster.getNamespace[IntRec, IntRec]("rangetestlimit")

        /* Insert Integers 1-100 */
        (1 to 100).foreach(i => ns.put(IntRec(i),IntRec(i)))

        /* Various range checks */
        ns.getRange(None, None, limit=10).map(_._1.f1)               should equal(1 to 10)
        ns.getRange(None, IntRec(50), limit=10).map(_._1.f1)         should equal(1 to 10)
        ns.getRange(IntRec(50), None, limit=10).map(_._1.f1)         should equal(50 until 60)
        ns.getRange(IntRec(10), IntRec(90), limit=10).map(_._1.f1)   should equal(10 until 20)
        ns.getRange(IntRec(-10), None, limit=10).map(_._1.f1)        should equal(1 to 10)
        ns.getRange(None, IntRec(110), limit=10).map(_._1.f1)        should equal(1 to 10)
        ns.getRange(IntRec(-10), IntRec(110), limit=10).map(_._1.f1) should equal(1 to 10)
      }

      it("should correctly return ranges backwards") {
        val ns = cluster.getNamespace[IntRec, IntRec]("rangetestbackwards")

        /* Insert Integers 1-100 */
        (1 to 100).foreach(i => ns.put(IntRec(i),IntRec(i)))

        /* Various range checks */
        ns.getRange(None, None, ascending=false).map(_._1.f1)               should equal(100 to 1 by -1)
        ns.getRange(None, IntRec(50), ascending=false).map(_._1.f1)         should equal(49 to 1 by -1)
        ns.getRange(IntRec(50), None, ascending=false).map(_._1.f1)         should equal(100 to 50 by -1)
        ns.getRange(IntRec(10), IntRec(90), ascending=false).map(_._1.f1)   should equal(89 to 10 by -1)
        ns.getRange(IntRec(-10), None, ascending=false).map(_._1.f1)        should equal(100 to 1 by -1)
        ns.getRange(None, IntRec(110), ascending=false).map(_._1.f1)        should equal(100 to 1 by -1)
        ns.getRange(IntRec(-10), IntRec(110), ascending=false).map(_._1.f1) should equal(100 to 1 by -1)

      }

      it("should correctly return ranges backwards with limit") {
        val ns = cluster.getNamespace[IntRec, IntRec]("rangetestlimitbackwards")
        /* Insert Integers 1-100 */
        (1 to 100).foreach(i => ns.put(IntRec(i),IntRec(i)))

        ns.getRange(None, None, limit=10, ascending=false).map(_._1.f1)               should equal(100 to 91 by -1)
        ns.getRange(None, IntRec(50), limit=10, ascending=false).map(_._1.f1)         should equal(49 to 40 by -1)
        ns.getRange(IntRec(50), None, limit=10, ascending=false).map(_._1.f1)         should equal(100 to 91 by -1)
        ns.getRange(IntRec(10), IntRec(90), limit=10, ascending=false).map(_._1.f1)   should equal(89 to 80 by -1)
        ns.getRange(IntRec(-10), None, limit=10, ascending=false).map(_._1.f1)        should equal(100 to 91 by -1)
        ns.getRange(None, IntRec(110), limit=10, ascending=false).map(_._1.f1)        should equal(100 to 91 by -1)
        ns.getRange(IntRec(-10), IntRec(110), limit=10, ascending=false).map(_._1.f1) should equal(100 to 91 by -1)

      }
    }

    it("should implement asyncGetRange") {

      val ns = cluster.getNamespace[IntRec, IntRec]("asyncgetrangetest")

      /* Insert Integers 1-100 */
      (1 to 100).foreach(i => ns.put(IntRec(i),IntRec(i)))

      /* Various range checks */
      ns.asyncGetRange(None, None).get.map(_._1.f1)               should equal(1 to 100)
      ns.asyncGetRange(None, IntRec(50)).get.map(_._1.f1)         should equal(1 until 50)
      ns.asyncGetRange(IntRec(50), None).get.map(_._1.f1)         should equal(50 to 100)
      ns.asyncGetRange(IntRec(10), IntRec(90)).get.map(_._1.f1)   should equal(10 until 90)
      ns.asyncGetRange(IntRec(-10), None).get.map(_._1.f1)        should equal(1 to 100)
      ns.asyncGetRange(None, IntRec(110)).get.map(_._1.f1)        should equal(1 to 100)
      ns.asyncGetRange(IntRec(-10), IntRec(110)).get.map(_._1.f1) should equal(1 to 100)

    }

    it("should return all versions") {
      val ns = cluster.getNamespace[IntRec, StringRec]("allversions")
      ns.put(IntRec(1), StringRec("string1"))
      val values = ns.getAllVersions(IntRec(1)).map(_._2.get.f1)
      values should contain ("string1")
    }

    it("should implement test/set") {pending}
    it("should correctly return a count of records") {pending}

    it("should handle prefix schema resolution automatically") {
      import scala.collection.JavaConversions._

      import org.apache.avro.{ generic, Schema }
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
    
      val ns = cluster.getNamespace[IntRec, StringRec]("pluspluseqtest")

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
