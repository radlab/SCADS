package com.googlecode.avro

import scala.tools.nsc
import nsc.util._
import nsc.Global
import nsc.Phase
import nsc.plugins.Plugin
import nsc.plugins.PluginComponent
import nsc.transform.Transform
import nsc.transform.TypingTransformers
import nsc.typechecker.Analyzer
import nsc.typechecker.Duplicators
import nsc.symtab.Flags._

import scala.collection.mutable.{HashMap,HashSet,MutableList}

import org.apache.avro.Schema

class AvroState(val recordClassSchemas: HashMap[String,Schema], val unions: HashMap[String,HashSet[String]])

class ScalaAvroPlugin(val global: Global) extends Plugin {
  import global._
  
  val avroRecordAnnotationClass = "com.googlecode.avro.annotation.AvroRecord"
  val avroUnionAnnotationClass = "com.googlecode.avro.annotation.AvroUnion"
  val name = "avro-scala-plugin"
  val description = "Support for auto generation of Avro Records"
  
  val unitsWithSynthetics = new MutableList[CompilationUnit]
  val unitsInError = new MutableList[CompilationUnit]

  val state = new AvroState(new HashMap[String,Schema], new HashMap[String,HashSet[String]])

  object earlyNamer extends PluginComponent {
	val global : ScalaAvroPlugin.this.global.type = ScalaAvroPlugin.this.global
    val phaseName = "earlynamer"
    val runsAfter = List[String]("parser")
    def newPhase(_prev: Phase): StdPhase = new StdPhase(_prev) {
      override val checkable = false
      def apply(unit: CompilationUnit) {
    	val silentReporter = new SilentReporter
    	val cachedReporter = global.reporter
    	global.reporter = silentReporter
    	
    	try {
	      import analyzer.{newNamer, rootContext}
	      newNamer(rootContext(unit)).enterSym(unit.body)
    	} finally {
          global.reporter = cachedReporter
          if (silentReporter.errorReported) {
        	unitsInError += unit
          }
    	}
      }
    }
  }
  
  object earlyTyper extends PluginComponent {
	val global : ScalaAvroPlugin.this.global.type = ScalaAvroPlugin.this.global
    val phaseName = "earlytyper"
    val runsAfter = List[String]()
    override val runsRightAfter = Some("earlynamer")
    def newPhase(_prev: Phase): StdPhase = new StdPhase(_prev) {
      import analyzer.{resetTyper, newTyper, rootContext}
      resetTyper()
      override def run { 
        currentRun.units foreach applyPhase
      }
      def apply(unit: CompilationUnit) {
    	val silentReporter = new SilentReporter
    	val cachedReporter = global.reporter
    	global.reporter = silentReporter
    	
        try {
          unit.body = newTyper(rootContext(unit)).typed(unit.body)
          if (global.settings.Yrangepos.value && !global.reporter.hasErrors) global.validatePositions(unit.body)
          for (workItem <- unit.toCheck) workItem()
        } finally {
          unit.toCheck.clear()
          global.reporter = cachedReporter
          if (silentReporter.errorReported) {
        	unitsInError += unit
          }
        }
      }
    }
  }
      
  val components = List[PluginComponent](
    new InitialTransformComponent(this, global),
    earlyNamer,
    earlyTyper,
    new GenerateSynthetics(this, global),
    new ErrorRetyper(this, global))
  
}
