package edu.berkeley.cs.scads

import edu.berkeley.cs.scads.model.parser.ScadsLanguage
import edu.berkeley.cs.scads.model.parser.Printer

object Compiler extends ScadsLanguage {
	def main(args: Array[String]) = {
		val src = scala.io.Source.fromFile(args(0)).getLines.foldLeft(new StringBuilder)((x: StringBuilder, y: String) => x.append(y)).toString

		parse(src) match {
			case Success(result, _) => {
				println(result)
				println(Printer(result))
			}
			case f: NoSuccess => {
				println("Parsing Failed")
				println(f)
			}
		}
	}
}
