package edu.berkeley.cs.scads.test

import org.specs._
import org.specs.runner.JUnit4

import edu.berkeley.cs.scads.model.parser._

object ErrorSpec extends SpecificationWithJUnit("Scads Compiler Error Specification"){
	val parser = new ScadsLanguage

	def bind(code: String) =
 		Binder.bind(parser.parse(code).get)	
	
	"The SCADS binder" should {
		"detect duplicate entities" in {
					bind("ENTITY User {string name PRIMARY(name)}\n ENTITY User {string name PRIMARY(name)}") must
						throwA[DuplicateEntityException]
				}	

		"detect duplicate attributes" in {skip("not written")}
		"detect duplicate relationships" in {skip("not written")}
		"detect unknown entities" in {skip("not written")}
		"detect duplicate queries" in {skip("not written")}
		"detect duplicate parameters" in {skip("not written")}
		"detect bad ordinals" in {skip("not written")}
		"detect ambiguious this params" in {skip("not written")}
		"detect unknown relationships" in {skip("not written")}
		"detect ambiguious joins" in {skip("not written")}
		"detect ambiguious attributes" in {skip("not written")}
		"detect unknown attributes" in {skip("not written")}
		"detect unknown fetch alias" in {skip("not written")}
		"detect inconsistent parameter typing" in {skip("not written")}
	}
}

class ErrorTest extends JUnit4(ErrorSpec)
