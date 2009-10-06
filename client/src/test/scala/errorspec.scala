package edu.berkeley.cs.scads.test

import org.specs._
import org.specs.runner.JUnit4

import edu.berkeley.cs.scads.Compiler
import edu.berkeley.cs.scads.model.parser._

object ErrorSpec extends SpecificationWithJUnit("Scads Compiler Error Specification"){
	"The SCADS binder" should {
		"detect duplicate entities" in {
			Compiler.codeGenFromSource("ENTITY User {string name, int nam PRIMARY(name)}\n ENTITY User {string name, int nam PRIMARY(name)}") must
				throwA[DuplicateEntityException]
		}
		"detect duplicate attributes" in {
			Compiler.codeGenFromSource("ENTITY User {string name, int name PRIMARY(name)}") must
				throwA[DuplicateAttributeException]
		}
		"detect duplicate relationships" in {
			Compiler.codeGenFromSource("ENTITY User {string name, int nam PRIMARY(name)}\nRELATIONSHIP x FROM User TO ONE User\n RELATIONSHIP x FROM User TO ONE User") must
				throwA[DuplicateRelationException]
		}
		"detect unknown entities" in {
			Compiler.codeGenFromSource("RELATIONSHIP x FROM unknown TO ONE unknown") must
				throwA[DuplicateEntityException]
		}
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
		"allow entities with only one attribute" in {skip("not written")}
	}
}

class ErrorTest extends JUnit4(ErrorSpec)
