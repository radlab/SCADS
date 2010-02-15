package edu.berkeley.cs.scads.piql

import java.io.File

import edu.berkeley.cs.scads.piql.parser._

object Compiler {
	object Parser extends ScadsLanguage

	def readFile(file: File): String = {
		val fis = new java.io.FileInputStream(file)
		val in = new java.io.BufferedReader(new java.io.InputStreamReader(fis, "UTF-8"))
		val ret = new StringBuilder
		var line: String = in.readLine()

		while(line != null) {
			ret.append(line)
			ret.append("\n")
			line = in.readLine()
		}

		return ret.toString()
	}

	def getAST(file: File): Spec = {
		getAST(readFile(file))
	}

	def getAST(piqlCode: String): Spec = {
		Parser.parse(piqlCode) match {
			case Parser.Success(result, _) => result
			case _ => throw new RuntimeException("Parse Failure")
		}
	}
}
