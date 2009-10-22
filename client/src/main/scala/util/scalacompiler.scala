package edu.berkeley.cs.scads.model.parser

import scala.tools.nsc.{Global, Settings}
import scala.tools.nsc.reporters.ConsoleReporter
import scala.tools.nsc.util.BatchSourceFile
import scala.tools.nsc.io.VirtualDirectory
import scala.tools.nsc.interpreter.AbstractFileClassLoader

/**
 * Allows a user to inject compiled scala source code into the running JVM, and provides a classloader for accessing them
 * TODO: this should probably be a class so that we can load changed versions of a given class without stopping the JVM. Right?
 * TODO: an interface to accesing the classes that doesn't require going straight to the classloader
 */
object ScalaCompiler {
  val settings = new Settings(error)
  settings.deprecation.value = true
  settings.unchecked.value = true

  val virtualDirectory = new VirtualDirectory("(memory)", None)
  val reporter = new ConsoleReporter(settings)
  val compiler = new Global(settings, reporter)
  val classLoader = new AbstractFileClassLoader(virtualDirectory, ClassLoader.getSystemClassLoader())
  compiler.genJVM.outputDir = virtualDirectory

  def compile(source: String): Unit = {
    val batchFile = new BatchSourceFile("<scads spec>", source.toCharArray)
    (new compiler.Run).compileSources(List(batchFile))
  }
}
