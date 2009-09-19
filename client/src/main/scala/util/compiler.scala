package edu.berkeley.cs.scads

import org.apache.log4j.Logger
import org.apache.log4j.BasicConfigurator
import java.io.FileWriter

import edu.berkeley.cs.scads.model.parser._


object Compiler extends ScadsLanguage {
	val logger = Logger.getLogger("scads.compiler")
	BasicConfigurator.configure()

	def main(args: Array[String]) = {
		logger.info("Loading spec.")
		val src = scala.io.Source.fromFile(args(0)).getLines.foldLeft(new StringBuilder)((x: StringBuilder, y: String) => x.append(y)).toString
		val outFile = new FileWriter("src/main/scala/generated/spec.scala")

		logger.info("Parsing spec.")
		parse(src) match {
			case Success(result, _) => {
				logger.debug("AST: " + Printer(result))
				try {
					val boundSpec = Binder.bind(result)
					logger.debug("Bound Spec: " + boundSpec)

					val source = ScalaGen(boundSpec)
					logger.debug(source)
					outFile.write(source)
				}
				catch {
					case UnknownRelationshipException(rn) => logger.fatal("Unknown relationship referenced: " + rn)
					case UnknownAttributeException(qn, an) => logger.fatal("Unknown attribute '" + an + "' in query " + qn)
					case UnknownEntityException(en) => logger.fatal("Unknown entity referenced: " + en)
					case BadParameterOrdinals(qn) => logger.fatal("Bad parameter ordinals detected in query " + qn)
					case UnsupportedPredicateException(qn, p) => logger.fatal("Query " + qn + " contains the following unsupported predicate " + p)
				}
				outFile.close()
			}
			case f: NoSuccess => {
				println("Parsing Failed")
				println(f)
			}
		}
	}
}
