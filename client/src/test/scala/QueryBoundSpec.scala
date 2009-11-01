package edu.berkeley.cs.scads.test

import org.specs._
import org.specs.runner.JUnit4

import edu.berkeley.cs.scads.Compiler
import edu.berkeley.cs.scads.model.parser._

object QueryBoundSpec extends SpecificationWithJUnit("Scads Optimizer Bounds Specification") {
	"The SCADS Optimizer" should {
		"accept queries with no limit when" >> {
			"its by primary key" in {
				Compiler.codeGenFromSource("ENTITY e1 {int a1, int a2 PRIMARY(a1)}\n QUERY unbounded FETCH e1 WHERE a1 = [1: p]") must
					not(throwA[UnboundedQuery])
			}
		}
		"throw an error for queries that return an unbounded number of results" >> {
			"from a secondary lookup" in {
				Compiler.codeGenFromSource("ENTITY e1 {int a1, int a2 PRIMARY(a1)}\n QUERY unbounded FETCH e1 WHERE a2 = [1: p]") must
					throwA[UnboundedFinalResult]
			}
		}
	}
}
class QueryBoundTest extends JUnit4(QueryBoundSpec)
