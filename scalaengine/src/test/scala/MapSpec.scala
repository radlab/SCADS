package edu.berkeley.cs.scads.test

import org.specs._
import org.specs.runner.JUnit4

import edu.berkeley.cs.scads.comm._
import edu.berkeley.cs.scads.comm.Conversions._

import edu.berkeley.cs.scads.storage._

object NamespaceSpec extends SpecificationWithJUnit("SCADS Namespace") {
  val intRec = new IntRec

  val gptest = TestScalaEngine.cluster.getNamespace[IntRec, IntRec]("getputtest")
  val fmtest = TestScalaEngine.cluster.getNamespace[IntRec, IntRec]("fmtest")
  val rangetest = TestScalaEngine.cluster.getNamespace[IntRec, IntRec]("rangetest")

  "A SCADS Map" should {
    "implement get/put" in {
      val (ir1, ir2) = (IntRec(1234), IntRec(5478))

      gptest.put(ir1, ir2)
      gptest.get(ir1) must_== Some(ir2)
    }

    "implement flatMap" in {
      (1 to 10).foreach(i => {
        fmtest.put(IntRec(i), IntRec(i * 10))
      })

      fmtest.flatMap {
        case (k,v) => if(k.f1 % 2 == 0) List(IntRec(v.f1/10)) else Nil
      } must haveTheSameElementsAs ((1 to 10).filter(_ % 2 == 0).map(IntRec(_)))
    }

    "getRange" in {
      val data = (1 to 10).map(i => IntRec(i) -> IntRec(i))
      rangetest ++= data

      rangetest.getRange(null, null) must haveTheSameElementsAs(data)
    }
  }
}
