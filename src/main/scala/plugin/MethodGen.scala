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

    /** AST nodes for runtime helper methods */

    /** this.mkUtf8(this.sym) */
    private def string2utf8(clazz: Symbol, sym: Symbol): Tree = {
        Apply(
            This(clazz) DOT newTermName("mkUtf8"),
            List(This(clazz) DOT sym))
    }

    /** this.mkByteBuffer(this.sym) */
    private def byteArray2byteBuffer(clazz: Symbol, sym: Symbol): Tree = {
        Apply(
            This(clazz) DOT newTermName("mkByteBuffer"),
            List(This(clazz) DOT sym))
    }

    /** this.scalaListToGenericArray(
     *  this.sym, this.getSchema.getField(sym.name).schema) */
    private def list2GenericArray(clazz: Symbol, sym: Symbol): Tree = {
        Apply(
            This(clazz) DOT newTermName("scalaListToGenericArray"),
            List(
                This(clazz) DOT sym,
                Apply(
                    Select(
                        Apply(
                            This(clazz) DOT newTermName("getSchema") DOT newTermName("getField"),
                            List(LIT(sym.name.toString.trim))),
                        newTermName("schema")),
                    List())))
    }

    /** this.sym.asInstanceOf[java.lang.Object] */
    private def sym2obj(clazz: Symbol, sym: Symbol): Tree = This(clazz) DOT sym AS ObjectClass.tpe

    /** Map symbol to symbol handler */
    private var symMap = Map(
      StringClass -> ((clazz:Symbol, sym:Symbol) => string2utf8(clazz,sym)),
      ArrayClass  -> ((clazz:Symbol, sym:Symbol) => 
        if (sym.tpe.typeArgs.head.typeSymbol == ByteClass)
            byteArray2byteBuffer(clazz,sym)
        else
            throw new UnsupportedOperationException("Cannot handle this right now")
        ),
      ListClass   -> ((clazz:Symbol, sym:Symbol) =>  list2GenericArray(clazz,sym)))

    private def generateGetMethod(templ: Template, clazz: Symbol, instanceVars: List[Symbol]) = {
      val newSym = clazz.newMethod(clazz.pos.focus, newTermName("get"))
      newSym setFlag SYNTHETICMETH | OVERRIDE 
      newSym setInfo MethodType(newSym.newSyntheticValueParams(List(/*Boxed*/ IntClass.tpe)), /*Any*/ObjectClass.tpe)
      clazz.info.decls enter newSym 

      val arg = newSym ARG 0
      // TODO: throw the avro bad index exception here
      val default = List(DEFAULT ==> THROW(IndexOutOfBoundsExceptionClass, arg))
      println(symMap)
      val cases = for ((sym, i) <- instanceVars.zipWithIndex) yield {
        CASE(LIT(i)) ==> {
          val fn = symMap get (sym.tpe.typeSymbol) getOrElse ( (c:Symbol, s:Symbol) => sym2obj(c,s) )
          fn(clazz, sym)
        }
      }

      localTyper.typed {
        DEF(newSym) === {
            arg MATCH { cases ::: default : _* }
        }   
      }   
    }

    private def generateSetMethod(templ: Template, clazz: Symbol, instanceVars: List[Symbol]) = {
      val newSym = clazz.newMethod(clazz.pos.focus, newTermName("put"))
      newSym setFlag SYNTHETICMETH 
      newSym setInfo MethodType(newSym.newSyntheticValueParams(List(IntClass.tpe, AnyClass.tpe)), UnitClass.tpe)
      clazz.info.decls enter newSym 

      // TODO: throw avro bad index class
      val default = List(DEFAULT ==> THROW(IndexOutOfBoundsExceptionClass, newSym ARG 0))

      val list = clazz.info.decls.toList filter (_ hasFlag ACCESSOR)

      val byteBufferTpe = byteBufferClass.tpe
      val utf8Tpe = utf8Class.tpe

      val cases = for ((sym, i) <- instanceVars.zipWithIndex) yield {
        val accessors = list.filter(_.name.toString.trim equals (sym.name.toString.trim + "_$eq"))
        val rhs = 
          if (sym.tpe.typeSymbol == StringClass) {
            typer typed ((newSym ARG 1) AS utf8Tpe DOT newTermName("toString"))
          } else if (sym.tpe.typeSymbol == ArrayClass && sym.tpe.normalize.typeArgs.head == ByteClass.tpe) {
            typer typed ((newSym ARG 1) AS byteBufferTpe.normalize DOT newTermName("array"))
          } else if (sym.tpe.typeSymbol == ListClass) {
            val apply = 
              Apply(
                  Select(
                      This(clazz),
                      newTermName("genericArrayToScalaList")),
                  List(Apply(
                      Select(
                          This(clazz),
                          newTermName("castToGenericArray")),
                      List(newSym ARG 1)))) AS sym.tpe
            typer typed apply
          } else {
            typer typed ((newSym ARG 1) AS sym.tpe)
          }
        val target = Assign(This(clazz) DOT sym, rhs)
        CASE(LIT(i)) ==> target
      }

      localTyper.typed {
        DEF(newSym) === {
            (newSym ARG 0) MATCH { cases ::: default : _* }
        }   
      }   
    }

    private def generateGetSchemaMethod(clazz: Symbol): Tree = {
      val newSym = clazz.newMethod(clazz.pos.focus, newTermName("getSchema"))
      newSym setFlag SYNTHETICMETH 
      newSym setInfo MethodType(newSym.newSyntheticValueParams(Nil), schemaClass.tpe)
      clazz.info.decls enter newSym 
      localTyper.typed {
        DEF(newSym) === {
          This(clazz.companionModule.moduleClass) DOT newTermName("schema")
        }
      }
    }

    override def transform(tree: Tree) : Tree = {
      val newTree = tree match {
        case cd @ ClassDef(mods, name, tparams, impl) if (cd.symbol.hasAnnotation(avroRecordAnnotation)) =>
          debug(retrieveRecordSchema(cd.symbol))

          val instanceVars = for (member <- impl.body if isValDef(member)) yield { member.symbol }
          val newMethods = List(
            generateGetMethod(impl, cd.symbol, instanceVars),
            generateSetMethod(impl, cd.symbol, instanceVars),
            generateGetSchemaMethod(cd.symbol))

          val newImpl = treeCopy.Template(impl, impl.parents, impl.self, newMethods ::: impl.body)
          treeCopy.ClassDef(cd, mods, name, tparams, newImpl)
        case _ => tree
      }
      super.transform(newTree)
    }    
  }
}
