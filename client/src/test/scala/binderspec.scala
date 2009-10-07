package edu.berkeley.cs.scads.test

import org.specs._
import org.specs.runner.JUnit4

import edu.berkeley.cs.scads.Compiler
import edu.berkeley.cs.scads.model.parser._

object BinderSpec extends SpecificationWithJUnit("Scads Compiler Error Specification"){
	"The SCADS binder" should {
		"throw an error for" >> {
			"duplicate entities" in {
				Compiler.codeGenFromSource("ENTITY User {string name, int nam PRIMARY(name)}\n ENTITY User {string name, int nam PRIMARY(name)}") must
					throwA[DuplicateEntityException]
			}
			"duplicate attributes" in {
				Compiler.codeGenFromSource("ENTITY User {string name, int name PRIMARY(name)}") must
					throwA[DuplicateAttributeException]
			}
			"duplicate relationships" in {
				Compiler.codeGenFromSource("ENTITY User {string name, int nam PRIMARY(name)}\nRELATIONSHIP x FROM User TO ONE User\n RELATIONSHIP x FROM User TO ONE User") must
					throwA[DuplicateRelationException]
			}
			"unknown entities" in {
				Compiler.codeGenFromSource("RELATIONSHIP x FROM unknown TO ONE unknown") must
					throwA[DuplicateEntityException]
			}
			"duplicate queries" in {skip("not written")}
			"duplicate parameters" in {skip("not written")}
			"bad ordinals" in {skip("not written")}
			"ambiguious this params" in {skip("not written")}
			"unknown relationships" in {skip("not written")}
			"ambiguious joins" in {skip("not written")}
			"ambiguious attributes" in {skip("not written")}
			"unknown attributes" in {skip("not written")}
			"unknown fetch alias" in {skip("not written")}
			"inconsistent parameter typing" in {skip("not written")}
			"invalid primary keys" in {
				Compiler.codeGenFromSource("ENTITY e1 {string s PRIMARY(r)}") must throwA[InvalidPrimaryKeyException]
				Compiler.codeGenFromSource("ENTITY e1 {string s PRIMARY(s,r)}") must throwA[InvalidPrimaryKeyException]
			}
		}
		"allow" >> {
			"entities with only one attribute" in {
				Compiler.codeGenFromSource("ENTITY User {string name PRIMARY(name)}") mustMatch("User")
			}
			"entities with primary keys that are part foreign key" in {
				Compiler.codeGenFromSource("ENTITY e1 {string s PRIMARY(s)}\nENTITY e2{string s PRIMARY(s,r)}\nRELATIONSHIP r FROM e1 TO MANY e2") mustMatch("e1")
			}
		}
	}
}

class BinderTest extends JUnit4(BinderSpec)
