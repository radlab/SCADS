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

import java.util.{Arrays => JArrays}

import scala.collection.JavaConversions._

import org.apache.avro.Schema

trait MethodGen extends ScalaAvroPluginComponent
                with    Transform
                with    TypingTransformers
                with    TreeDSL {
  import global._
  import definitions._
  	  
  val runsAfter = List[String]("objectgen")
  override val runsRightAfter = Some("objectgen")
  val phaseName = "methodgen"
  def newTransformer(unit: CompilationUnit) = new MethodGenTransformer(unit)    

  class MethodGenTransformer(unit: CompilationUnit) extends TypingTransformer(unit) {
    import CODE._

    /** this.sym.asInstanceOf[java.lang.Object] */
    private def castToObject(clazz: Symbol, sym: Symbol): Tree = 
      This(clazz) DOT sym AS ObjectClass.tpe

    private def construct(tpe: Type, args: List[Type]) = tpe match {
      case TypeRef(pre, sym, _) => TypeRef(pre, sym, args)
    }

    private def toAvroType(tpe: Type): Type = {
      if (tpe.typeSymbol.isSubClass(TraversableClass)) {
        // TODO: this is a bit fragile (for instance, what if somebody defines
        // their map as a type alias. there won't be any type params
        val size = tpe.typeArgs.size
        size match {
          case 1 =>
            construct(GenericArrayClass.tpe, List(toAvroType(tpe.typeArgs.head)))
          case 2 =>
            construct(JMapClass.tpe, List(utf8Class.tpe, toAvroType(tpe.typeArgs.tail.head)))
          case _ =>
            throw new UnsupportedOperationException("Unsupported collection type: " + tpe)
        }
      } else tpe.typeSymbol match {
        case OptionClass =>
          toAvroType(tpe.typeArgs.head)
        case StringClass => utf8Class.tpe 
        case ArrayClass  =>
          if (tpe.typeArgs.head.typeSymbol == ByteClass)
            byteBufferClass.tpe
          else throw new UnsupportedOperationException("Arrays not used for lists: use scala collections instead")
        case ByteClass | BoxedByteClass | 
             ShortClass | BoxedShortClass | 
             CharClass | BoxedCharacterClass | 
             IntClass | BoxedIntClass  => BoxedIntClass.tpe 
        case BooleanClass | BoxedBooleanClass => BoxedBooleanClass.tpe
        case LongClass | BoxedLongClass  => BoxedLongClass.tpe
        case FloatClass | BoxedFloatClass  => BoxedFloatClass.tpe
        case DoubleClass | BoxedDoubleClass => BoxedDoubleClass.tpe
        case _ => tpe
      }
    }
    
    private def needsSchemaToConvert(tpe: Type): Boolean = 
      tpe.typeSymbol.isSubClass(TraversableClass) || 
      (tpe.typeSymbol == OptionClass && needsSchemaToConvert(tpe.typeArgs.head))

    private def canInline(tpe: Type) = 
      tpe == toAvroType(tpe) || (tpe.typeSymbol match {
        case IntClass | LongClass | BooleanClass | FloatClass | DoubleClass => true
        case _ => false
      })

    private def generateGetMethod(templ: Template, clazz: Symbol, instanceVars: List[Symbol]) = {
      val newSym = clazz.newMethod(clazz.pos.focus, newTermName("get"))
      newSym setFlag SYNTHETICMETH | OVERRIDE 
      newSym setInfo MethodType(newSym.newSyntheticValueParams(List(/*Boxed*/ IntClass.tpe)), /*Any*/ObjectClass.tpe)
      clazz.info.decls enter newSym 

      val arg = newSym ARG 0
      // TODO: throw the avro bad index exception here
      val default = List(DEFAULT ==> THROW(IndexOutOfBoundsExceptionClass, arg))
      val cases = for ((sym, i) <- instanceVars.zipWithIndex) yield {
        CASE(LIT(i)) ==> {
          if (canInline(sym.tpe)) // trivial ones where no conversions are necessary
            castToObject(clazz, sym)
          else {
            val avroTpe = toAvroType(sym.tpe)
            val schema = 
              if (needsSchemaToConvert(sym.tpe))
                Apply(
                  Apply(
                    This(clazz) DOT newTermName("getSchema") DOT newTermName("getFields") DOT newTermName("get"),
                    List(LIT(i))) DOT newTermName("schema"),
                  Nil)
              else LIT(null)
            Apply(
              TypeApply(
                This(clazz) DOT newTermName("convert"),
                List(
                  TypeTree(sym.tpe),
                  TypeTree(avroTpe))),
                List(schema, This(clazz) DOT sym)) AS ObjectClass.tpe
          }
        }
      }

      atOwner(clazz)(localTyper.typed {
        DEF(newSym) === {
          arg MATCH { cases ::: default : _* }
        }   
      })   
    }

    private def generateSetMethod(templ: Template, clazz: Symbol, instanceVars: List[Symbol]) = {
      val newSym = clazz.newMethod(clazz.pos.focus, newTermName("put"))
      newSym setFlag SYNTHETICMETH | OVERRIDE
      newSym setInfo MethodType(newSym.newSyntheticValueParams(List(IntClass.tpe, AnyClass.tpe)), UnitClass.tpe)
      clazz.info.decls enter newSym 

      // TODO: throw avro bad index class
      val default = List(DEFAULT ==> THROW(IndexOutOfBoundsExceptionClass, newSym ARG 0))

      val byteBufferTpe = byteBufferClass.tpe
      val utf8Tpe = utf8Class.tpe

      val cases = for ((sym, i) <- instanceVars.zipWithIndex) yield {
        val rhs =
          if (canInline(sym.tpe))
            ((newSym ARG 1) AS sym.tpe)
          else {
            val avroTpe = toAvroType(sym.tpe)
            val schema = 
              if (needsSchemaToConvert(sym.tpe))
                Apply(
                  Apply(
                    This(clazz) DOT newTermName("getSchema") DOT newTermName("getFields") DOT newTermName("get"),
                    List(LIT(i))) DOT newTermName("schema"),
                  Nil)
              else LIT(null)
            Apply(
              TypeApply(
                This(clazz) DOT newTermName("convert"),
                List(
                  TypeTree(avroTpe),
                  TypeTree(sym.tpe))),
                List(schema, (newSym ARG 1) AS avroTpe))
          }
        CASE(LIT(i)) ==> Assign(This(clazz) DOT sym, rhs)
      }

      atOwner(clazz)(localTyper.typed {
        DEF(newSym) === {
            (newSym ARG 0) MATCH { cases ::: default : _* }
        }   
      })
    }

    private def generateGetSchemaMethod(clazzTree: ClassDef): Tree = {
      val clazz = clazzTree.symbol
      val newSym = clazz.newMethod(clazz.pos.focus, newTermName("getSchema"))
      newSym setFlag SYNTHETICMETH | OVERRIDE
      newSym setInfo MethodType(newSym.newSyntheticValueParams(Nil), schemaClass.tpe)
      clazz.info.decls enter newSym 
      //println("localTyper.context1.enclClass: " + localTyper.context1.enclClass)
      //println("companionModuleOf(clazz): " + companionModuleOf(clazzTree))
      //println("companionModuleOf(clazz).moduleClass: " + companionModuleOf(clazzTree.symbol).moduleClass)

      val innerTree: Tree =  /** Not sure why compiler needs type information (Tree) here */
        if (clazz.owner.isClass) { /** Only when the owner of this class is another class can we perform
                                    *  the optimization of referencing the companion object */
            debug("--- clazz ---: " + clazz)
            debug("clazz.toplevelClass: " + clazz.toplevelClass)
            debug("clazz.outerClass: " + clazz.outerClass)
            debug("clazz.enclClass: " + clazz.enclClass)
            if (clazz.toplevelClass == clazz) {
              This(companionModuleOf(clazzTree.symbol).moduleClass) DOT newTermName("schema")
            } else {
              This(clazz.outerClass) DOT newTermName(clazz.name.toString) DOT newTermName("schema")
            }
        } else { /** Fall back to the naive version in the other cases */
          // TODO: change getSchema to be a lazy val here instead (so we can
          // at least cache the invocations)
          warning("Unable to optimize getSchema method for class %s".format(clazz.fullName.toString))
          Apply(
            Ident(newTermName("org")) DOT 
              newTermName("apache")   DOT
              newTermName("avro")     DOT
              newTermName("Schema")   DOT
              newTermName("parse"),
            List(LIT(retrieveRecordSchema(clazz).get.toString)))
        }
      localTyper.typed {
        DEF(newSym) === { innerTree }
      }
    }

    private def generateGetUnionSchemaMethod(clazzTree: ClassDef, unionSchema: Schema): Tree = {
      val clazz = clazzTree.symbol
      val newSym = clazz.newMethod(clazz.pos.focus, newTermName("getSchema"))
      newSym setFlag SYNTHETICMETH | OVERRIDE
      newSym setInfo MethodType(newSym.newSyntheticValueParams(Nil), schemaClass.tpe)
      clazz.info.decls enter newSym 

      localTyper.typed {
        DEF(newSym) === { 
          Apply(
            Ident(newTermName("org")) DOT 
              newTermName("apache")   DOT
              newTermName("avro")     DOT
              newTermName("Schema")   DOT
              newTermName("parse"),
            List(LIT(unionSchema.toString)))
        }
      }
    }


    override def transform(tree: Tree) : Tree = {
      val newTree = tree match {
        case cd @ ClassDef(mods, name, tparams, impl) if (cd.symbol.tpe.parents.contains(avroUnionTrait.tpe)) =>
          //println("FOUND UNION: " + cd.symbol)
          val schema = getOrCreateUnionSchema(cd.symbol, 
              Schema.createUnion(JArrays.asList(retrieveUnionRecords(cd.symbol).map(s => retrieveRecordSchema(s).get).toArray:_*)))
          //println("SCHEMA: " + schema)
          cd.symbol.resetFlag(INTERFACE) /** force compiler to generate backing $class class */
          val newMethod = List(generateGetUnionSchemaMethod(cd, schema))
          val newImpl = treeCopy.Template(impl, impl.parents, impl.self, newMethod ::: impl.body)
          treeCopy.ClassDef(cd, mods, name, tparams, newImpl)
        case cd @ ClassDef(mods, name, tparams, impl) if (cd.symbol.tpe.parents.contains(avroRecordTrait.tpe)) =>
          debug(retrieveRecordSchema(cd.symbol))
          debug(cd.symbol.fullName + "'s enclClass: " + cd.symbol.enclClass)
          debug("owner.enclClass: " + cd.symbol.owner.enclClass)
          debug("toplevelClass: " + cd.symbol.toplevelClass)
          debug("owner.toplevelClass: " + cd.symbol.owner.toplevelClass)

          val instanceVars = for (member <- impl.body if isValDef(member)) yield { member.symbol }
          val newMethods = List(
            generateGetMethod(impl, cd.symbol, instanceVars),
            generateSetMethod(impl, cd.symbol, instanceVars),
            generateGetSchemaMethod(cd))

          val newImpl = treeCopy.Template(impl, impl.parents, impl.self, newMethods ::: impl.body)
          treeCopy.ClassDef(cd, mods, name, tparams, newImpl)
        case _ => tree
      }
      super.transform(newTree)
    }    
  }
}
