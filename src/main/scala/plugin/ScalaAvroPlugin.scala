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

import org.apache.avro.{specific, Schema}
import Schema.Type
import specific.SpecificRecord

trait ScalaAvroPluginComponent extends PluginComponent {
  import global._
  import global.definitions

  protected def debug(a: AnyRef) {
    if (settings.debug.value) 
      println(a)
    //println(a)
  }

  protected def warn(a: AnyRef) {
    println(a)
  }

  protected def isValDef(tree: Tree): Boolean = tree.isInstanceOf[ValDef] 
  protected def isVar(tree: Tree): Boolean = isVarSym(tree.symbol)
  protected def isVarSym(symbol: Symbol): Boolean = symbol.hasFlag(MUTABLE)

  // TODO: the following values are lazy so that a NPE doesn't get thrown
  // upon instantiation

  /** Definitions doesn't contain one for MapClass */
  protected lazy val MapClass = definitions.getClass("scala.collection.immutable.Map")

  /** Avro Scala Plugin Traits */
  protected lazy val avroRecordTrait = definitions.getClass("com.googlecode.avro.marker.AvroRecord")
  protected lazy val avroUnionTrait = definitions.getClass("com.googlecode.avro.marker.AvroUnion")

  /** Avro Extra Primitive Types */
  protected lazy val byteBufferClass = definitions.getClass("java.nio.ByteBuffer")
  protected lazy val utf8Class = definitions.getClass("org.apache.avro.util.Utf8")

  /** Avro Internal Types */
  protected lazy val GenericArrayClass = definitions.getClass("org.apache.avro.generic.GenericArray")
  protected lazy val schemaClass = definitions.getClass("org.apache.avro.Schema")
  protected lazy val SpecificRecordIface = definitions.getClass("org.apache.avro.specific.SpecificRecord")
  protected lazy val SpecificRecordBaseClass = definitions.getClass("org.apache.avro.specific.SpecificRecordBase")
  protected lazy val JMapClass = definitions.getClass("java.util.Map")

  /** Scala Avro Internal types */
  protected lazy val ScalaSpecificRecord = definitions.getClass("com.googlecode.avro.runtime.ScalaSpecificRecord")
  protected lazy val AvroConversions = definitions.getClass("com.googlecode.avro.runtime.HasAvroConversions")
  protected lazy val GenericArrayWrapperClass = definitions.getClass("com.googlecode.avro.runtime.GenericArrayWrapper")

  /** Takes a class symbol and maps to its associated Schema object */
  protected val classToSchema: Map[Symbol, Schema]

  /** Takes a union trait and maps to all its extenders */
  protected val unionToExtenders: Map[Symbol, List[Symbol]]

  /** Takes a union trait and maps to its Avro schema object */
  protected val unionToSchemas: Map[Symbol, Schema]

  /** Takes a compilation unit (a source file) and maps to all union traits
   * inside it*/
  protected val unitMap: Map[CompilationUnit, List[Symbol]]
  
  /** Takes a symbol name for a module and maps it to its companion class */
  protected val companionClassMap: Map[String, Symbol]

  /** Takes a symbol name for a module and maps it to its module symbol */
  protected val companionModuleMap: Map[String, Symbol]

  protected def retrieveRecordSchema(name: Symbol): Option[Schema] = classToSchema.get(name)

  protected def addRecordSchema(name: Symbol, schema: Schema) {
    assert(schema.getType == Type.RECORD)
    classToSchema += name -> schema
  }

  protected def isRecord(sym: Symbol) = classToSchema.contains(sym)

  protected def isExternalRecord(sym: Symbol) = sym.tpe <:< SpecificRecordIface.tpe

  protected def retrieveExternalRecordSchema(sym: Symbol): Schema = {
    val clazz = Class.forName(sym.fullName.toString).asInstanceOf[Class[SpecificRecord]]
    clazz.newInstance.getSchema
  }

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

  protected def getOrCreateUnionSchema(sym: Symbol, schema: => Schema): Schema =
    unionToSchemas.getOrElseUpdate(sym, schema)

  protected def getUnionSchema(sym: Symbol) = 
    unionToSchemas.get(sym)

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

  protected def companionClassOf(module: Symbol): Symbol = {
    val companionClass0 = module.companionClass
    if (companionClass0 != NoSymbol) {
      companionClass0
    } else {
      companionClassMap.get(module.fullName).getOrElse(NoSymbol)
    }
  }

  protected def companionModuleOf(clazz: Symbol): Symbol = {
    val companionModule0 = clazz.companionModule
    if (companionModule0 != NoSymbol) {
      companionModule0
    } else {
      companionModuleMap.get(clazz.fullName).getOrElse(NoSymbol)
    }
  }

}

class ScalaAvroPlugin(val global: Global) extends Plugin {
  import global._

  val name = "avro-scala-plugin"
  val description = "Support for auto generation of Avro Records"
  
  val classToSchema: Map[Symbol, Schema] = new HashMap[Symbol, Schema]
  val unionToExtenders: Map[Symbol, List[Symbol]] = new HashMap[Symbol, List[Symbol]]
  val unionToSchemas: Map[Symbol, Schema] = new HashMap[Symbol, Schema]
  val unitMap: Map[CompilationUnit, List[Symbol]] = new HashMap[CompilationUnit, List[Symbol]]
  val companionModuleMap: Map[String, Symbol] = new HashMap[String, Symbol]
  val companionClassMap: Map[String, Symbol] = new HashMap[String, Symbol]

  object extender extends Extender {
    val global : ScalaAvroPlugin.this.global.type = ScalaAvroPlugin.this.global
    val classToSchema = ScalaAvroPlugin.this.classToSchema
    val unionToExtenders = ScalaAvroPlugin.this.unionToExtenders
    val unionToSchemas = ScalaAvroPlugin.this.unionToSchemas
    val unitMap = ScalaAvroPlugin.this.unitMap
    val companionModuleMap = ScalaAvroPlugin.this.companionModuleMap
    val companionClassMap = ScalaAvroPlugin.this.companionClassMap
  }

  object ctorRetype extends CtorRetype {
    val global : ScalaAvroPlugin.this.global.type = ScalaAvroPlugin.this.global
    val classToSchema = ScalaAvroPlugin.this.classToSchema
    val unionToExtenders = ScalaAvroPlugin.this.unionToExtenders
    val unionToSchemas = ScalaAvroPlugin.this.unionToSchemas
    val unitMap = ScalaAvroPlugin.this.unitMap
    val companionModuleMap = ScalaAvroPlugin.this.companionModuleMap
    val companionClassMap = ScalaAvroPlugin.this.companionClassMap
  }

  object unionDiscover extends UnionDiscover {
    val global : ScalaAvroPlugin.this.global.type = ScalaAvroPlugin.this.global
    val classToSchema = ScalaAvroPlugin.this.classToSchema
    val unionToExtenders = ScalaAvroPlugin.this.unionToExtenders
    val unionToSchemas = ScalaAvroPlugin.this.unionToSchemas
    val unitMap = ScalaAvroPlugin.this.unitMap
    val companionModuleMap = ScalaAvroPlugin.this.companionModuleMap
    val companionClassMap = ScalaAvroPlugin.this.companionClassMap
  }

  object unionClosure extends UnionClosure {
    val global : ScalaAvroPlugin.this.global.type = ScalaAvroPlugin.this.global
    val classToSchema = ScalaAvroPlugin.this.classToSchema
    val unionToExtenders = ScalaAvroPlugin.this.unionToExtenders
    val unionToSchemas = ScalaAvroPlugin.this.unionToSchemas
    val unitMap = ScalaAvroPlugin.this.unitMap
    val companionModuleMap = ScalaAvroPlugin.this.companionModuleMap
    val companionClassMap = ScalaAvroPlugin.this.companionClassMap
  }

  object schemaCreate extends SchemaCreate {
    val global : ScalaAvroPlugin.this.global.type = ScalaAvroPlugin.this.global
    val classToSchema = ScalaAvroPlugin.this.classToSchema
    val unionToExtenders = ScalaAvroPlugin.this.unionToExtenders
    val unionToSchemas = ScalaAvroPlugin.this.unionToSchemas
    val unitMap = ScalaAvroPlugin.this.unitMap
    val companionModuleMap = ScalaAvroPlugin.this.companionModuleMap
    val companionClassMap = ScalaAvroPlugin.this.companionClassMap
  }

  object schemaGen extends SchemaGen {
    val global : ScalaAvroPlugin.this.global.type = ScalaAvroPlugin.this.global
    val classToSchema = ScalaAvroPlugin.this.classToSchema
    val unionToExtenders = ScalaAvroPlugin.this.unionToExtenders
    val unionToSchemas = ScalaAvroPlugin.this.unionToSchemas
    val unitMap = ScalaAvroPlugin.this.unitMap
    val companionModuleMap = ScalaAvroPlugin.this.companionModuleMap
    val companionClassMap = ScalaAvroPlugin.this.companionClassMap
  }

  object objectGen extends ObjectGen {
    val global : ScalaAvroPlugin.this.global.type = ScalaAvroPlugin.this.global
    val classToSchema = ScalaAvroPlugin.this.classToSchema
    val unionToExtenders = ScalaAvroPlugin.this.unionToExtenders
    val unionToSchemas = ScalaAvroPlugin.this.unionToSchemas
    val unitMap = ScalaAvroPlugin.this.unitMap
    val companionModuleMap = ScalaAvroPlugin.this.companionModuleMap
    val companionClassMap = ScalaAvroPlugin.this.companionClassMap
  }

  object methodGen extends MethodGen {
    val global : ScalaAvroPlugin.this.global.type = ScalaAvroPlugin.this.global
    val classToSchema = ScalaAvroPlugin.this.classToSchema
    val unionToExtenders = ScalaAvroPlugin.this.unionToExtenders
    val unionToSchemas = ScalaAvroPlugin.this.unionToSchemas
    val unitMap = ScalaAvroPlugin.this.unitMap
    val companionModuleMap = ScalaAvroPlugin.this.companionModuleMap
    val companionClassMap = ScalaAvroPlugin.this.companionClassMap
  }

  val components = List[PluginComponent](
    extender,
    ctorRetype,
    unionDiscover,
    unionClosure,
    schemaCreate,
    schemaGen,
    objectGen,
    methodGen)
  
}
