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
				Compiler.codeGenFromSource("ENTITY e1 {string s PRIMARY(s)}\nENTITY e2 {string s PRIMARY(s)}\nRELATIONSHIP dup FROM e1 TO 1 e2\n RELATIONSHIP dup FROM e1 TO 1 e2") must
					throwA[DuplicateRelationException]
			}
			"unknown entities" in {
				Compiler.codeGenFromSource("RELATIONSHIP x FROM unknown TO 1 unknown") must
					throwA[UnknownEntityException]
			}
			"duplicate queries" in {
				Compiler.codeGenFromSource("ENTITY e1 {string s PRIMARY(s)}\nQUERY x FETCH e1 WHERE s=[1:s]\nQUERY x FETCH e1 WHERE s=[1:s]") must
					throwA[DuplicateQueryException]
			}
			"duplicate parameters" in {
				Compiler.codeGenFromSource("ENTITY e1 {string s PRIMARY(s)}\nQUERY x FETCH e1 WHERE s=[1:s] AND s=[2:s]") must
					throwA[DuplicateParameterException]
			}
			"bad ordinals" in {
				Compiler.codeGenFromSource("ENTITY e1 {string s PRIMARY(s)}\nQUERY x FETCH e1 WHERE s=[2:s]") must
					throwA[BadParameterOrdinals]
			}
			"ambiguious this params" in {
				Compiler.codeGenFromSource("ENTITY e1 {string s PRIMARY(s)}\nENTITY e2 {string s PRIMARY(s)}\nRELATIONSHIP r FROM e1 TO 1 e2\nQUERY x FETCH e1 OF e2 BY r WHERE e1=[this] AND e2=[this]") must
					throwA[AmbigiousThisParameter]
			}
			"unknown relationships" in {
				Compiler.codeGenFromSource("ENTITY e1 {string s PRIMARY(s)}\nENTITY e2 {string s PRIMARY(s)}\nQUERY x FETCH e1 OF e2 BY r WHERE e1=[this]") must
					throwA[UnknownRelationshipException]
			}
			"ambiguious joins" in {
				Compiler.codeGenFromSource("ENTITY e1 {string s PRIMARY(s)}\nENTITY e2 {string s PRIMARY(s)}\nRELATIONSHIP r FROM e1 TO 1 e2\nQUERY x FETCH e1 alias OF e2 alias BY r WHERE e1=[this]") must
					throwA[AmbiguiousJoinAlias]
			}
			"ambiguious attributes" in {
				Compiler.codeGenFromSource("ENTITY e1 {string s PRIMARY(s)}\nENTITY e2 {string s PRIMARY(s)}\nRELATIONSHIP r FROM e1 TO 1 e2\nQUERY x FETCH e1 OF e2 BY r WHERE s=[1:s]") must
					throwA[AmbiguiousAttribute]
			}
			"unknown attributes" in {
				Compiler.codeGenFromSource("ENTITY e1 {string s PRIMARY(s)}\nQUERY x FETCH e1 WHERE e1.x = [1:x]") must
					throwA[UnknownAttributeException]
				Compiler.codeGenFromSource("ENTITY e1 {string s PRIMARY(s)}\nQUERY x FETCH e1 WHERE x = [1:x]") must
					throwA[UnknownAttributeException]
			}
			"unknown fetch alias" in {
				Compiler.codeGenFromSource("ENTITY e1 {string s PRIMARY(s)}\nENTITY e2 {string s PRIMARY(s)}\nRELATIONSHIP r FROM e1 TO 1 e2\nQUERY x FETCH e1 OF e2 alias BY r WHERE a.x = [1:p]") must
					throwA[UnknownFetchAlias]
			}
			"inconsistent parameter typing" in {
				Compiler.codeGenFromSource("ENTITY e1 {string s, int n PRIMARY(s)}\nQUERY x FETCH e1 WHERE s = [1:x] AND n = [1:x]") must
					throwA[InconsistentParameterTyping]
			}
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
