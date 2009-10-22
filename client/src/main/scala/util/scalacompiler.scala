package edu.berkeley.cs.scads.model.parser

import scala.tools.nsc.{Global, Settings}
import scala.tools.nsc.reporters.ConsoleReporter
import scala.tools.nsc.util.BatchSourceFile
import scala.tools.nsc.io.VirtualDirectory
import scala.tools.nsc.interpreter.AbstractFileClassLoader

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
