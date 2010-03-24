package edu.berkeley.cs.scads.test

import org.specs._
import org.specs.runner.JUnit4

import edu.berkeley.cs.scads.piql.{Compiler, Entity}

object EntitySpec extends SpecificationWithJUnit("Scads Entity Specification"){
  lazy val e1 = Compiler.getClassLoader("""
      ENTITY e1 {
        string sf1,
        int if1
        PRIMARY(sf1, if1)
      }
  """).loadClass("e1").asInstanceOf[Class[Entity]]

  "PIQL Entities" should {
    "be instantiable" in {
      val a = e1.newInstance()
      true must_== true
    }
  }
}

class EntityTest extends JUnit4(EntitySpec)
