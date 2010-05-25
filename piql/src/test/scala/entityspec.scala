package edu.berkeley.cs.scads.test

import org.specs._
import org.specs.runner.JUnit4

import edu.berkeley.cs.scads.piql.{Compiler, Entity}
import edu.berkeley.cs.scads.storage.{TestScalaEngine}
import org.apache.avro.specific.SpecificRecordBase
import edu.berkeley.cs.scads.piql.DynamicDispatch._

object EntitySpec extends SpecificationWithJUnit("Scads Entity Specification") {
  implicit val cluster = TestScalaEngine.cluster

  lazy val e1 = Compiler.getClassLoader("""
      ENTITY e1 {
        string sf1,
        int if1
        PRIMARY(sf1)
      }
  """).loadClass("e1").asInstanceOf[Class[Entity[SpecificRecordBase, SpecificRecordBase]]]

  "PIQL Entities" should {
    "be instantiable" in {
      val a = e1.newInstance()
      true must_== true
    }

   "serialize field values" in {
      val a = e1.newInstance()
      a.setField("sf1", "test")
      a.setField("if1", new java.lang.Integer(1))

      val b = e1.newInstance()
      b.key.parse(a.key.toBytes)
      b.value.parse(a.value.toBytes)

      b.getField("sf1") must_== "test"
      b.getField("if1") must_== 1
    }

    "retain field values through load/store" in {
      val a = e1.newInstance()
      a.setField("sf1", "test")
      a.setField("if1", new java.lang.Integer(1))
      //a.save

      val b = e1.newInstance()
      //b.load(a.key)

      b.getField("sf1") must_== "test"
      b.getField("if1") must_== 1
    }
  }
}

class EntityTest extends JUnit4(EntitySpec)
