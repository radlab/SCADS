package edu.berkeley.cs.scads

import edu.berkeley.cs.scads.model.parser.ScadsLanguage

object Compiler {
	def main(args: Array[String]) = {
		val src = scala.io.Source.fromFile(args(0)).getLines.foldLeft(new StringBuilder)((x: StringBuilder, y: String) => x.append(y)).toString

		val p = new ScadsLanguage
		val result = p.parseAll(p.spec, src).get

		println(result.generateSpec)
	}
}
