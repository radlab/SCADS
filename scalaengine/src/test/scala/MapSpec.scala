package edu.berkeley.cs.scads.test

import org.specs._
import org.specs.runner.JUnit4

import edu.berkeley.cs.scads.comm._
import edu.berkeley.cs.scads.comm.Conversions._

import edu.berkeley.cs.scads.storage._

object MapSpec extends SpecificationWithJUnit("KeyStore Specification") {
  val intRec = new IntRec

  val gptest = TestScalaEngine.cluster.getNamespace[IntRec, IntRec]("getputtest")
  val fmtest = TestScalaEngine.cluster.getNamespace[IntRec, IntRec]("fmtest")

  object IntRec {
    def apply(i: Int): IntRec = {
      val r = new IntRec
      r.f1 = i
      r
    }
  }

  "A SCADS Map" should {
    "implement get/put" in {
      val (ir1, ir2) = (new IntRec, new IntRec)
      ir1.f1 = 1234
      ir2.f1 = 5678

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
  }
}
