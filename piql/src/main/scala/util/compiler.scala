package edu.berkeley.cs.scads.piql

import java.io.File
import org.apache.log4j.Logger

import edu.berkeley.cs.scads.piql.parser._

object PIQL2SQL {
  def main(args: Array[String]): Unit = {
    val piql = Compiler.readFile(new File(args(0)))
    val ast = Compiler.getAST(piql)
    val boundAst = new Binder(ast).bind
    val opt = new Optimizer(boundAst).optimizedSpec
    println(DDLGen(opt))
    println(DMLGen(opt))
  }
}

object Compiler {
  val logger = Logger.getLogger("scads.piql.compiler")
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
			case failure => throw new RuntimeException("Parse Failure: " + failure)
		}
	}

  def getClassLoader(piqlCode: String): ClassLoader = {
    val ast = getAST(piqlCode)
    val boundAst = new Binder(ast).bind
    val opt = new Optimizer(boundAst).optimizedSpec
    val compiler = new ScalaCompiler
    val code = ScalaGen(opt)
    logger.debug("==GENERATED SCALA CODE==")
    logger.debug(code)

    compiler.compile(code)
    compiler.classLoader
  }
}
