package edu.berkeley.cs.scads.piql.parser

import org.apache.log4j.Logger

import scala.tools.nsc.{Global, Settings}
import scala.tools.nsc.reporters.ConsoleReporter
import scala.tools.nsc.util.BatchSourceFile
import scala.tools.nsc.io.VirtualDirectory
import scala.tools.nsc.interpreter.AbstractFileClassLoader

/**
 * Allows a user to inject compiled scala source code into the running JVM, and provides a classloader for accessing them
 * TODO: an interface to accesing the classes that doesn't require going straight to the classloader
 */
class ScalaCompiler {
	val logger = Logger.getLogger("scads.scalacompiler")
  val virtualDirectory = new VirtualDirectory("(memory)", None)

  val settings = new Settings(error)
  settings.deprecation.value = true
  settings.unchecked.value = true
  settings.outputDirs.setSingleOutput(virtualDirectory)

	/* If we are running inside maven surefire, the actual classpath has been cleared so use the one for this test */
	if(System.getProperty("surefire.test.class.path") != null)
		settings.classpath.value = System.getProperty("surefire.test.class.path")
  else
    settings.classpath.value = System.getProperty("java.class.path")

  logger.debug("ScalaCompiler classpath: " + settings.classpath.value)

  val reporter = new ConsoleReporter(settings)
  val compiler = new Global(settings, reporter)
  val classLoader = new AbstractFileClassLoader(virtualDirectory, Thread.currentThread.getContextClassLoader())

  def compile(source: String): Unit = {
		logger.debug("Begining scala snipit compilation")
    val batchFile = new BatchSourceFile("<scala snipit>", source.toCharArray)
    (new compiler.Run).compileSources(List(batchFile))
		logger.debug("Snipit compilation complete")
  }
}
