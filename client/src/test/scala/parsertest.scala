package edu.berkeley.cs.scads.test

import org.scalatest.Suite
import org.apache.log4j.Logger

import scala.tools.nsc.Interpreter
import scala.tools.nsc.Settings

import edu.berkeley.cs.scads.model.parser.ScadsLanguage

class ParserTest extends Suite {
	val lang = new ScadsLanguage
	val logger = Logger.getLogger("scads.parserTest")

	val code = Map(
	"simple code" -> """
	ENTITY Profile {
		string username,
		string first_name,
		int coolness
		PRIMARY(username)
	}""",
	"code w/ multiple entities" -> """
	ENTITY Profile {
		string username,
		string first_name,
		int coolness
		PRIMARY(username)
	}
	ENTITY User {
		string username,
		string first_name,
		int coolness
		PRIMARY(username)
	}""")

	val badCode = Map(
		"duplicate entities" -> """
		ENTITY User {
			string username
			PRIMARY(username)
		}
		ENTITY User {
			string username
			PRIMARY(username)
		}""",

		"no primary key" -> """
		ENTITY User {
			string username,
		}"""
	)

	def testParseGoodCode() = {
		code.foreach((c) => {
			lang.parse(c._2).get
		})
	}

	def testFailOnBadCode() = {
		badCode.foreach((c) => {
			intercept[RuntimeException] {
				lang.parse(c._2).get
			}
		})
	}

	def testGeneratesValidCode() = {
		System.setProperty("java.class.path", "/Users/marmbrus/.m2/repository/org/scala-lang/scala-library/2.7.5/scala-library-2.7.5.jar:/Users/marmbrus/.m2/repository/jetty/jetty-util/6.0.2/jetty-util-6.0.2.jar:/Users/marmbrus/.m2/repository/jetty/jetty/6.0.2/jetty-6.0.2.jar:/Users/marmbrus/.m2/repository/commons-collections/commons-collections/3.1/commons-collections-3.1.jar:/Users/marmbrus/.m2/repository/log4j/log4j/1.2.15/log4j-1.2.15.jar:/Users/marmbrus/.m2/repository/com/sun/jmx/jmxri/1.2.1/jmxri-1.2.1.jar:/Users/marmbrus/Workspace/scads/placement/target/classes:/Users/marmbrus/.m2/repository/oro/oro/2.0.8/oro-2.0.8.jar:/Users/marmbrus/.m2/repository/javax/activation/activation/1.1/activation-1.1.jar:/Users/marmbrus/.m2/repository/velocity/velocity/1.5/velocity-1.5.jar:/Users/marmbrus/.m2/repository/com/sun/jdmk/jmxtools/1.2.1/jmxtools-1.2.1.jar:/Users/marmbrus/Workspace/scads/placement/target/test-classes:/Users/marmbrus/.m2/repository/org/apache/thrift/libthrift/0.1-20090610-xtrace/libthrift-0.1-20090610-xtrace.jar:/Users/marmbrus/.m2/repository/javax/servlet/servlet-api/2.4/servlet-api-2.4.jar:/Users/marmbrus/.m2/repository/edu/berkeley/cs/scads/thrift-interface/1.0.2/thrift-interface-1.0.2.jar:/Users/marmbrus/.m2/repository/javax/mail/mail/1.4/mail-1.4.jar:/Users/marmbrus/.m2/repository/org/scala-tools/testing/scalatest/0.9.5/scalatest-0.9.5.jar:/Users/marmbrus/.m2/repository/javax/jms/jms/1.1/jms-1.1.jar:/Users/marmbrus/.m2/repository/commons-lang/commons-lang/2.1/commons-lang-2.1.jar:/Users/marmbrus/.m2/repository/edu/berkeley/xtrace/xtrace/2.1-20090610/xtrace-2.1-20090610.jar:/Users/marmbrus/.m2/repository/org/scala-lang/scala-compiler/2.7.5/scala-compiler-2.7.5.jar")

		val settings = new Settings
		val interp = new Interpreter(settings)
		logger.debug("Classpath: " + settings.classpath)

		code.foreach((c) => {
			val ast = lang.parse(c._2).get
			fail("Code generation isn't implemented!")
			assert(true)
		})
	}
}
