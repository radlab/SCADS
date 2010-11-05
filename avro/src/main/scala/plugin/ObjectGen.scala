package edu.berkeley.cs.avro
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

      def fieldNameAsVal(fieldName: String) = 
        "%s ".format(fieldName)

      def fieldNameAsGetter(fieldName: String) =
        fieldName

      def newSchemaVal(fieldName: String, schema: String) = {
        val valSym = clazz.newValue(clazz.pos.focus, newTermName(fieldNameAsVal(fieldName)))
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
              List(LIT(schema)))
          }
        }
        (valSym, valDef)
      }

      def newSchemaGetter(fieldName: String) = {
        val getterSym = clazz.newMethod(clazz.pos.focus, newTermName(fieldNameAsGetter(fieldName)))
        getterSym setFlag (METHOD | STABLE | ACCESSOR)
        getterSym setInfo MethodType(getterSym.newSyntheticValueParams(Nil), schemaClass.tpe)
        clazz.info.decls enter getterSym

        val getDef = atOwner(clazz)(localTyper.typed {
          DEF(getterSym) === { THIS(clazz) DOT newTermName(fieldNameAsVal(fieldName)) }
        })
        (getterSym, getDef)
      }


      val (valSym, valDef) = newSchemaVal("schema", retrieveRecordSchema(companionClassOf(md.symbol)).get.toString)

      val (keyPairVal, valPairVal) = 
        if (isMarkedPair(companionClassOf(md.symbol))) {
          val (offset, keySchema, valueSchema) = classToKVSchema(companionClassOf(md.symbol))
          (Some(newSchemaVal("keySchema", keySchema.toString)), Some(newSchemaVal("valueSchema", valueSchema.toString)))
        } else 
          (None, None)

      // !!! HACK !!!
      val owner0 = localTyper.context1.enclClass.owner
      localTyper.context1.enclClass.owner = clazz

      val (getSym, getDef) = newSchemaGetter("schema")

      val (keyPairDef, valPairDef) =
        if (isMarkedPair(companionClassOf(md.symbol))) 
          (Some(newSchemaGetter("keySchema")), Some(newSchemaGetter("valueSchema")))
        else 
          (None, None)

      // !!! RESTORE HACK !!!
      localTyper.context1.enclClass.owner = owner0

      val mbrs = List(
        keyPairDef.map(d => List(keyPairVal.get._2, d._2)).getOrElse(Nil),
        valPairDef.map(d => List(valPairVal.get._2, d._2)).getOrElse(Nil),
        valDef :: getDef :: impl.body).flatten

      val impl0 = treeCopy.Template(impl, impl.parents, impl.self, mbrs)
      treeCopy.ModuleDef(md, md.mods, md.name, impl0)
    }

    override def transform(tree: Tree) : Tree = {
      val newTree = tree match {
        case md @ ModuleDef(mods, name, impl) if (isMarked(companionClassOf(md.symbol))) =>

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
