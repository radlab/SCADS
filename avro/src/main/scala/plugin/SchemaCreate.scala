package com.googlecode.avro
package plugin

import scala.tools.nsc._
import scala.tools.nsc.plugins.PluginComponent

import scala.collection.mutable.HashSet

import org.apache.avro.Schema

trait SchemaCreate extends ScalaAvroPluginComponent {
  import global._

  val runsAfter = List[String]("unionclosure")
  override val runsRightAfter = Some("unionclosure")
  val phaseName = "schemacreate"

  def newPhase(prev: Phase): Phase = new TraverserPhase(prev)
  class TraverserPhase(prev: Phase) extends StdPhase(prev) {
    def apply(unit: CompilationUnit) {
      newTraverser().traverse(unit.body)
    }   
  }

  def newTraverser(): Traverser = new ForeachTreeTraverser(check)

  def check(tree: Tree): Unit = tree match {
    case cd @ ClassDef(_, _, _, _) if (cd.symbol.tpe.parents.contains(avroRecordTrait.tpe)) =>
      val sym = cd.symbol
      val namespace = 
        if (sym.owner.fullName == "<empty>") None // Issue #6 - handle default package classes
        else Some(sym.owner.fullName)
      debug("Adding schema for class %s, namespace %s".format(sym.fullName, namespace))
      addRecordSchema(sym, 
          Schema.createRecord(sym.name.toString, "Auto-generated schema", namespace.orNull, false))
      debug("Registering class in companionClassMap")
      companionClassMap += sym.fullName -> sym
      debug("companionClassMap: " + companionClassMap) 
    case _ => ()
  }
}

