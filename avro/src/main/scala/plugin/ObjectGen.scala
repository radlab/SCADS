package com.googlecode.avro
package plugin

import scala.tools._
import nsc.Global
import nsc.Phase
import nsc.plugins.Plugin
import nsc.plugins.PluginComponent
import nsc.transform.Transform
import nsc.transform.InfoTransform
import nsc.transform.TypingTransformers
import nsc.symtab.Flags._
import nsc.util.Position
import nsc.util.NoPosition
import nsc.ast.TreeDSL
import nsc.typechecker
import scala.annotation.tailrec

import scala.collection.JavaConversions._

trait ObjectGen extends ScalaAvroPluginComponent
                with    Transform
                with    TypingTransformers
                with    TreeDSL {
  import global._
  import definitions._
      
  val runsAfter = List[String]("schemagen")
  override val runsRightAfter = Some("schemagen")
  val phaseName = "objectgen"
  def newTransformer(unit: CompilationUnit) = new ObjectGenTransformer(unit)    

  class ObjectGenTransformer(unit: CompilationUnit) extends TypingTransformer(unit) {
    import CODE._

    private def pimpModuleDef(md: ModuleDef): Tree = {
      debug("pimping the module def: " + md)

      val impl  = md.impl
      val clazz = md.symbol.moduleClass

      val valSym = clazz.newValue(clazz.pos.focus, newTermName("schema "))
      valSym setFlag (PRIVATE | LOCAL)
      valSym setInfo schemaClass.tpe
      clazz.info.decls enter valSym

      val valDef = localTyper.typed {
        VAL(valSym) === {
          Apply(
            Ident(newTermName("org")) DOT 
              newTermName("apache")   DOT
              newTermName("avro")     DOT
              newTermName("Schema")   DOT
              newTermName("parse"),
            List(LIT(retrieveRecordSchema(companionClassOf(md.symbol)).get.toString)))
        }
      }

      // !!! HACK !!!
      val owner0 = localTyper.context1.enclClass.owner
      localTyper.context1.enclClass.owner = clazz

      val getterSym = clazz.newMethod(clazz.pos.focus, newTermName("schema"))
      getterSym setFlag (METHOD | STABLE | ACCESSOR)
      getterSym setInfo MethodType(getterSym.newSyntheticValueParams(Nil), schemaClass.tpe)
      clazz.info.decls enter getterSym

      val getDef = atOwner(clazz)(localTyper.typed {
        DEF(getterSym) === { THIS(clazz) DOT newTermName("schema ") }
      })

      // !!! RESTORE HACK !!!
      localTyper.context1.enclClass.owner = owner0

      val impl0 = treeCopy.Template(impl, impl.parents, impl.self, valDef :: getDef :: impl.body)
      treeCopy.ModuleDef(md, md.mods, md.name, impl0)
    }

    override def transform(tree: Tree) : Tree = {
      val newTree = tree match {
        case md @ ModuleDef(mods, name, impl) if (companionClassOf(md.symbol).tpe.parents.contains(avroRecordTrait.tpe)) =>

          debug("Adding module to companionModule map")
          companionModuleMap += md.symbol.fullName -> md.symbol
          debug("companionModuleMap: " + companionModuleMap)

          val hasSchemaDef = ! ( impl.body.filter { 
            case ValDef(_, n, _, _) if (n.toString == "schema ") => true
            case _ => false
          } isEmpty )

          if (hasSchemaDef)
            throw new AssertionError("Object cannot have schema member already defined: %s".format(md.symbol))

          pimpModuleDef(md)
        case _ => tree
      }
      super.transform(newTree)
    }    
  }

}
