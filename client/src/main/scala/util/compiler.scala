package edu.berkeley.cs.scads

import edu.berkeley.cs.scads.model.parser.ScadsLanguage
import edu.berkeley.cs.scads.model.parser.Printer

object Compiler {
	def main(args: Array[String]) = {
		val src = scala.io.Source.fromFile(args(0)).getLines.foldLeft(new StringBuilder)((x: StringBuilder, y: String) => x.append(y)).toString
		val result = ScadsLanguage.parse(src).get

		println(result)
		println(Printer(result))
	}
}
