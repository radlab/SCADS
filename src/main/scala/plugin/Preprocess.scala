package com.googlecode.avro

import scala.tools.nsc._
import scala.tools.nsc.plugins.PluginComponent

import scala.collection.mutable.HashSet

class Preprocess(val plugin: ScalaAvroPlugin, val global: Global) extends PluginComponent {
  import global._
  import global.definitions._

  val runsAfter = List[String]("parser")
  override val runsRightAfter = Some("parser")
  val phaseName = "preprocessor"

  def newPhase(prev: Phase): Phase = new TraverserPhase(prev)
  class TraverserPhase(prev: Phase) extends StdPhase(prev) {
    def apply(unit: CompilationUnit) {
      newTraverser().traverse(unit.body)
    }
  }

  def newTraverser(): Traverser = new ForeachTreeTraverser(check)

  val modulesSeen = new HashSet[String]

  def check(tree: Tree): Unit = tree match {
    case md @ ModuleDef(mods, name, impl) =>
      modulesSeen.add(name.toString)
      if (plugin.state.hasObjClass.contains(name.toString))
        plugin.state.hasObjClass += name.toString -> true
      ()
    case cd @ ClassDef(mods, name, tparams, impl) =>
      if (!mods.annotations.exists( a => {
        a match {
            case Apply(Select(New(sym),_),_) =>
                println("sym found: " + sym)
                sym.toString.indexOf("AvroRecord") != -1
        }
      })) return
      if (modulesSeen.contains(name.toString))
        plugin.state.hasObjClass += name.toString -> true
      else 
        plugin.state.hasObjClass += name.toString -> false
      ()
    case _ => ()
  }
}
