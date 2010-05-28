package com.googlecode.avro
package plugin

import scala.tools.nsc._
import scala.tools.nsc.plugins.PluginComponent

import scala.collection.mutable.HashSet

import org.apache.avro.Schema

import scala.collection.mutable.{HashMap,HashSet,MutableList,ListBuffer}

trait UnionDiscover extends ScalaAvroPluginComponent {
  import global._
  import global.definitions._

  val runsAfter = List[String]("earlytyper")
  override val runsRightAfter = Some("earlytyper")
  val phaseName = "uniondiscover"

  def newPhase(prev: Phase): Phase = new TraverserPhase(prev)

  class TraverserPhase(prev: Phase) extends StdPhase(prev) {
    var unit: CompilationUnit = _
    def apply(_unit: CompilationUnit) {
      unit = _unit
      newTraverser().traverse(unit.body)
    }   

    def newTraverser(): Traverser = new ForeachTreeTraverser(check)

    def check(tree: Tree): Unit = tree match {
      case cd @ ClassDef(mods, _, _, _) if (cd.symbol.hasAnnotation(avroUnionAnnotation)) =>
        if (!mods.isSealed)
          throw new NonSealedClassException(cd.symbol.fullName)
        // TODO: what do we do if it's not abstract?
        debug("Adding union " + cd.symbol.fullName + "to unit mapping")
        addUnionToUnit(unit, cd.symbol)
      case _ => ()
    }
  }
}


