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

    override def transform(tree: Tree) : Tree = {
      val newTree = tree match {
        case md @ ModuleDef(mods, name, impl) if (md.symbol.companionClass.hasAnnotation(avroRecordAnnotation)) =>
          debug("generating schema obj for module: " + md.symbol.fullName)

          val clazz = md.symbol.moduleClass

          val valSym = clazz.newValue(clazz.pos.focus, newTermName("schema "))
          valSym setFlag PRIVATE | MUTABLE | LOCAL
          //valSym setFlag FINAL
          //valSym setInfo TypeRef(ThisType(nme.lang), StringClass, Nil)
          valSym setInfo schemaClass.tpe
          clazz.info.decls enter valSym
          //println("mbr: " + clazz.tpe.member(newTermName("schema ")))

          val valDef = atOwner(clazz)(localTyper.typed {
            VAL(valSym) === {
              Apply(
                Ident(newTermName("org")) DOT 
                  newTermName("apache")   DOT
                  newTermName("avro")     DOT
                  newTermName("Schema")   DOT
                  newTermName("parse"),
                List(LIT(retrieveRecordSchema(md.symbol.companionClass).get.toString)))
            }
          })

          debug("valDef: " + valDef)
          
          val owner0 = localTyper.context1.enclClass.owner
          localTyper.context1.enclClass.owner = clazz

          val getterSym = clazz.newMethod(clazz.pos.focus, newTermName("schema"))
          getterSym setFlag METHOD | ACCESSOR
          getterSym setInfo MethodType(getterSym.newSyntheticValueParams(Nil), schemaClass.tpe)
          clazz.info.decls enter getterSym

          val getDef = atOwner(clazz)(localTyper.typed {
              DEF(getterSym) === { THIS(clazz) DOT newTermName("schema ") }
          })

          val setterSym = clazz.newMethod(clazz.pos.focus, newTermName("schema_$eq"))
          setterSym setFlag METHOD | ACCESSOR
          setterSym setInfo MethodType(setterSym.newSyntheticValueParams(List(schemaClass.tpe)), UnitClass.tpe)
          clazz.info.decls enter setterSym

          val setDef = atOwner(clazz)(localTyper.typed {
              DEF(setterSym) === { 
                  (THIS(clazz) DOT newTermName("schema ")) === (setterSym ARG 0)
              }
          })

          localTyper.context1.enclClass.owner = owner0

          val (car, cdr) = impl.body.splitAt(1)
          val newImpl = treeCopy.Template(impl, impl.parents, impl.self, car ::: List(valDef, getDef, setDef) ::: cdr)
          treeCopy.ModuleDef(md, mods, name, newImpl)  
        case _ => tree
      }
      super.transform(newTree)
    }    
  }

}
