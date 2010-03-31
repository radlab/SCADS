package edu.berkeley.cs.scads.test

import org.specs._
import org.specs.runner.JUnit4

import edu.berkeley.cs.scads.comm._
import edu.berkeley.cs.scads.comm.Conversions._

import edu.berkeley.cs.scads.storage._

object MapSpec extends SpecificationWithJUnit("KeyStore Specification") {
  val intRec = new IntRec

  //Note: this object is a hack to prevent specs from trying to double create the namespaces.
  object NS {
    val gptest = TestScalaEngine.cluster.createNamespace[IntRec, IntRec]("getputtest", intRec.getSchema(), intRec.getSchema(), List(TestScalaEngine.node))
    val fmtest = TestScalaEngine.cluster.createNamespace[IntRec, IntRec]("fmtest", intRec.getSchema(), intRec.getSchema(), List(TestScalaEngine.node))
  }

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

      NS.gptest.put(ir1, ir2)
      NS.gptest.get(ir1) must_== Some(ir2)
    }

    "implement flatMap" in {
      (1 to 10).foreach(i => {
        NS.fmtest.put(IntRec(i), IntRec(i * 10))
      })

      NS.fmtest.flatMap {
        case (k,v) => if(k.f1 % 2 == 0) List(IntRec(v.f1/10)) else Nil
      } must haveTheSameElementsAs ((1 to 10).filter(_ % 2 == 0).map(IntRec(_)))

    }
  }
}
