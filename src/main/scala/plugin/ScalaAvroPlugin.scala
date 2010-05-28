package com.googlecode.avro
package plugin

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

import scala.collection.mutable.{Map,HashMap,HashSet,MutableList,ListBuffer}

import org.apache.avro.Schema
import Schema.Type

trait ScalaAvroPluginComponent extends PluginComponent {
  import global._
  import global.definitions

  protected def debug(a: AnyRef) {
    if (settings.debug.value) log(a)
    println(a)
  }

  protected def isValDef(tree: Tree): Boolean = tree.isInstanceOf[ValDef] 
  protected def isVar(tree: Tree): Boolean = isVarSym(tree.symbol)
  protected def isVarSym(symbol: Symbol): Boolean = symbol.hasFlag(MUTABLE)

  // TODO: the following values are lazy so that a NPE doesn't get thrown
  // upon instantiation

  /** Avro Scala Plugin Annotations */
  protected lazy val avroRecordAnnotation = definitions.getClass("com.googlecode.avro.annotation.AvroRecord")
  protected lazy val avroUnionAnnotation = definitions.getClass("com.googlecode.avro.annotation.AvroUnion")

  /** Avro Extra Primitive Types */
  protected lazy val byteBufferClass = definitions.getClass("java.nio.ByteBuffer")
  protected lazy val utf8Class = definitions.getClass("org.apache.avro.util.Utf8")

  /** Avro Internal Types */
  protected lazy val schemaClass = definitions.getClass("org.apache.avro.Schema")

  /** Takes a class symbol and maps to its associated Schema object */
  protected val classToSchema: Map[Symbol, Schema]

  /** Takes a union trait and maps to all its extenders */
  protected val unionToExtenders: Map[Symbol, List[Symbol]]

  /** Takes a compilation unit (a source file) and maps to all union traits
   * inside it*/
  protected val unitMap: Map[CompilationUnit, List[Symbol]]

  protected def retrieveRecordSchema(name: Symbol): Option[Schema] = classToSchema.get(name)

  protected def addRecordSchema(name: Symbol, schema: Schema) {
    assert(schema.getType == Type.RECORD)
    classToSchema += name -> schema
  }

  protected def isRecord(sym: Symbol) = classToSchema.contains(sym)

  protected def retrieveUnionRecords(name: Symbol): List[Symbol] = unionToExtenders.get(name) match {
    case Some(l) => l
    case None => Nil
  }

  protected def addUnionRecord(name: Symbol, schema: Symbol) {
    unionToExtenders.get(name) match { 
      case Some(buf) =>
        unionToExtenders += name -> (buf ::: List(schema))
      case None =>
        unionToExtenders += name -> List(schema)
    }
  }

  protected def isUnion(sym: Symbol) = unionToExtenders.contains(sym)

  protected def retrieveUnions(unit: CompilationUnit): List[Symbol] = unitMap.get(unit) match {
    case Some(l) => l
    case None => Nil
  }

  protected def addUnionToUnit(unit: CompilationUnit, union: Symbol) {
    unitMap.get(unit) match {
      case Some(buf) =>
        unitMap += unit -> (buf ::: List(union))
      case None =>
        unitMap += unit -> List(union)
    }
  }

}

class ScalaAvroPlugin(val global: Global) extends Plugin {
  import global._

  val name = "avro-scala-plugin"
  val description = "Support for auto generation of Avro Records"
  
  // TODO: i dont think unitsWithSynthetics is in use anywhere
  val unitsWithSynthetics = new MutableList[CompilationUnit]
  val unitsInError = new MutableList[CompilationUnit]

  val classToSchema: Map[Symbol, Schema] = new HashMap[Symbol, Schema]
  val unionToExtenders: Map[Symbol, List[Symbol]] = new HashMap[Symbol, List[Symbol]]
  val unitMap: Map[CompilationUnit, List[Symbol]] = new HashMap[CompilationUnit, List[Symbol]]

  object extender extends Extender {
    val global : ScalaAvroPlugin.this.global.type = ScalaAvroPlugin.this.global
    val classToSchema = ScalaAvroPlugin.this.classToSchema
    val unionToExtenders = ScalaAvroPlugin.this.unionToExtenders
    val unitMap = ScalaAvroPlugin.this.unitMap
  }

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

  object unionDiscover extends UnionDiscover {
    val global : ScalaAvroPlugin.this.global.type = ScalaAvroPlugin.this.global
    val classToSchema = ScalaAvroPlugin.this.classToSchema
    val unionToExtenders = ScalaAvroPlugin.this.unionToExtenders
    val unitMap = ScalaAvroPlugin.this.unitMap
  }

  object unionClosure extends UnionClosure {
    val global : ScalaAvroPlugin.this.global.type = ScalaAvroPlugin.this.global
    val classToSchema = ScalaAvroPlugin.this.classToSchema
    val unionToExtenders = ScalaAvroPlugin.this.unionToExtenders
    val unitMap = ScalaAvroPlugin.this.unitMap
  }

  object schemaCreate extends SchemaCreate {
    val global : ScalaAvroPlugin.this.global.type = ScalaAvroPlugin.this.global
    val classToSchema = ScalaAvroPlugin.this.classToSchema
    val unionToExtenders = ScalaAvroPlugin.this.unionToExtenders
    val unitMap = ScalaAvroPlugin.this.unitMap
  }

  object schemaGen extends SchemaGen {
    val global : ScalaAvroPlugin.this.global.type = ScalaAvroPlugin.this.global
    val classToSchema = ScalaAvroPlugin.this.classToSchema
    val unionToExtenders = ScalaAvroPlugin.this.unionToExtenders
    val unitMap = ScalaAvroPlugin.this.unitMap
  }

  object objectGen extends ObjectGen {
    val global : ScalaAvroPlugin.this.global.type = ScalaAvroPlugin.this.global
    val classToSchema = ScalaAvroPlugin.this.classToSchema
    val unionToExtenders = ScalaAvroPlugin.this.unionToExtenders
    val unitMap = ScalaAvroPlugin.this.unitMap
  }

  object methodGen extends MethodGen {
    val global : ScalaAvroPlugin.this.global.type = ScalaAvroPlugin.this.global
    val classToSchema = ScalaAvroPlugin.this.classToSchema
    val unionToExtenders = ScalaAvroPlugin.this.unionToExtenders
    val unitMap = ScalaAvroPlugin.this.unitMap
  }

  object errorRetyper extends ErrorRetyper {
    val global : ScalaAvroPlugin.this.global.type = ScalaAvroPlugin.this.global
  }
      
  val components = List[PluginComponent](
    extender,
    earlyNamer,
    earlyTyper,
    unionDiscover,
    unionClosure,
    schemaCreate,
    schemaGen,
    objectGen,
    methodGen,
    errorRetyper)
  
}
