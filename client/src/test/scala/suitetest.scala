package edu.berkeley.cs.scads.test

import org.scalatest.Suite
import edu.berkeley.cs.scads.Compiler

class SpecTest extends Suite {
	def testScadr() {
		Compiler.main(Array("scadr.scads"))
	}

	def testScadbook() {
		Compiler.main(Array("scadbook.scads"))
	}

	def testScadbay() {
		Compiler.main(Array("scadbay.scads"))
	}
}
