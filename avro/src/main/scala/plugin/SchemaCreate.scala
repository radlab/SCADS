package edu.berkeley.cs.avro
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
    case cd @ ClassDef(_, _, _, _) if isMarked(cd) =>
      val sym = cd.symbol
      val namespace = 
        if (sym.owner.fullName == "<empty>") None // Issue #6 - handle default package classes
        else Some(sym.owner.fullName)
      debug("Adding schema for class %s, namespace %s".format(sym.fullName, namespace))
      val DOC = "Auto-generated schema"
      val clzName = sym.name.toString
      addRecordSchema(sym, 
          Schema.createRecord(clzName, DOC, namespace.orNull, false))
      if (isMarkedPair(cd)) {
        val numKeyParams = {
          val innerCtorTpe = cd.symbol.primaryConstructor.tpe
          innerCtorTpe.resultType match {
            case MethodType(outerParams, _) =>
              innerCtorTpe.params.size + outerParams.size 
            case _ => 
              innerCtorTpe.params.size
          }
        }
        addPairSchemas(sym,
            numKeyParams,
            Schema.createRecord(
              clzName + "KeyType", 
              DOC,
              namespace.orNull,
              false),
            Schema.createRecord(
              clzName + "ValueType",
              DOC,
              namespace.orNull,
              false))
      }
      debug("Registering class in companionClassMap")
      companionClassMap += sym.fullName -> sym
      debug("companionClassMap: " + companionClassMap) 
    case _ => ()
  }
}

