package com.googlecode.avro
package plugin

import scala.tools.nsc._

import org.apache.avro.Schema

trait UnionClosure extends ScalaAvroPluginComponent {
  import global._

  val runsAfter = List[String]("uniondiscover")
  override val runsRightAfter = Some("uniondiscover")
  val phaseName = "unionclosure"

  def newPhase(prev: Phase): Phase = new TraverserPhase(prev)

  class TraverserPhase(prev: Phase) extends StdPhase(prev) {
    var unit: CompilationUnit = _
    def apply(_unit: CompilationUnit) {
      unit = _unit
      debug("Union for comp unit: " + unit)
      debug(retrieveUnions(unit))
      newTraverser().traverse(unit.body)
    }   

    def newTraverser(): Traverser = new ForeachTreeTraverser(check)

    def check(tree: Tree): Unit = tree match {
      case cd @ ClassDef(_, _, _, _) if (cd.symbol.tpe.parents.contains(avroRecordTrait.tpe)) =>
        debug("Running union closure on class: " + cd.symbol.fullName)
        val unitUnions = retrieveUnions(unit)
        unitUnions.
          filter(unionSym => {
            debug("Comparing cd.symbol.tpe " + cd.symbol.tpe + " to " + unionSym.tpe)
            cd.symbol.tpe <:< unionSym.tpe }).
          foreach(unionSym => addUnionRecord(unionSym, cd.symbol))
      case cd @ ClassDef(mods,_,_,_) =>
        debug("Skipped class: " + cd.symbol.fullName)
      case _ => ()
    }
  }

}
