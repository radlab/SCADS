package com.googlecode.avro

import scala.tools.nsc.symtab.Flags._
import scala.tools.nsc._
import scala.tools.nsc.plugins.PluginComponent
import scala.tools.nsc.transform.Transform
import scala.tools.nsc.util.{Position, NoPosition}
import scala.tools.nsc.ast.TreeDSL

class InitialTransformComponent(plugin: ScalaAvroPlugin, val global: Global) extends PluginComponent
                                                     with Transform 
                                                     with TreeDSL {
  import global._
  import global.definitions._

  val runsAfter = List[String]("parser")
  override val runsRightAfter = Some("parser")
  val phaseName = "initialtransform"

  def newTransformer(unit: CompilationUnit) = new InitialTransformer(unit)

  class InitialTransformer(val unit: CompilationUnit) extends /*Typing*/ Transformer {
    import CODE._

    def preTransform(tree: Tree): Tree = tree match {
      case cd @ ClassDef(mods, name, tparams, impl) =>
          println("mods: " + mods)
          println("tparams: " + tparams)
          println("impl.self: " + impl.self)
          println("impl.parents: " + impl.parents)
          println("impl.body: " + impl.body)

          /*
          // TODO: Need to find a way to check the class of the annotation
          // (the type symbol) instead of just doing a naive string comparsion
          // Essentially, we'll need to run another earlynamer/earlytyper here
          // Below is not sufficient (but a good start)
          val annotationNamer = global.analyzer.newNamer(global.analyzer.rootContext(unit))
          val annotationTyper = global.analyzer.newTyper(global.analyzer.rootContext(unit))
          if (!mods.annotations.exists( a => {
            a match {
                case Apply(Select(New(sym),_),_) =>
                    println("sym found: " + sym)
                    //sym.toString == plugin.avroRecordAnnotationClass 
                    annotationNamer.enterSym(a)
                    val typedAnnotation = annotationTyper.typed(a)
                    println("tpe.normalize: " + typedAnnotation.tpe.normalize)
                    typedAnnotation.tpe.normalize.toString == plugin.avroRecordAnnotationClass
            }
          })) return tree
          */

          if (!mods.annotations.exists( a => {
            a match {
                case Apply(Select(New(sym),_),_) =>
                    println("sym found: " + sym)
                    sym.toString.indexOf("AvroRecord") != -1
            }
          })) return tree

          println("preTransform classdef: " + name)
          //println("template: " + impl)

          //val qual = SelectStart(
          //            Select(
          //                Select(
          //                    Select(
          //                        Ident(newTypeName("org")), 
          //                    newTypeName("apache")),
          //                  newTypeName("avro")),
          //                newTypeName("util")))

          //val utf8Import = Import(
          //        qual,
          //        //Select(
          //        //    Ident(newTypeName("util")),
          //        //    Select(
          //        //        Ident(newTypeName("avro")),
          //        //        Select(
          //        //            Ident(newTypeName("apache")),
          //        //            newTypeName("org")))),
          //        //Ident(newTypeName("org.apache.avro.util")),
          //        List(ImportSelector(newTypeName("Utf8"),tree.pos.startOrPoint,newTypeName("Utf8"),tree.pos.startOrPoint)))
          ////utf8Import.symbol.setPos(cd.pos.focus)
          ////cd.symbol.info.decls enter utf8Import
          //  //utf8Import.setSymbol(NoSymbol.newImport(NoPosition).setFlag(SYNTHETIC).setInfo( global.analyzer.ImportType(qual))).setType(NoType)
          //val importWithPos = atPos(tree.pos)(utf8Import)

          // def mkUtf8(p:String) = new org.apache.avro.util.Utf8(p)
          // TODO: check for p == null
          val mkUtf8 = DefDef(
            NoMods,
            newTermName("mkUtf8"),
            List(),
            List(
                List(
                    ValDef(
                        NoMods,
                        newTermName("p"),
                        Ident(newTypeName("String")), 
                        EmptyTree))),
            TypeTree(),
            Apply(
                Select(
                    New(
                      Select(
                        Select(
                            Select(
                                Select(
                                    Ident(newTermName("org")), 
                                newTermName("apache")),
                                newTermName("avro")),
                            newTermName("util")),
                        newTypeName("Utf8"))),
                    newTermName("<init>")),
                List(
                    Ident(newTermName("p")))))
          val mkUtf8WithPos = atPos(tree.pos)(mkUtf8)

          // def mkByteBuffer(bytes: Array[Byte]) = java.nio.ByteBuffer.wrap(bytes)
          // TODO: check for bytes == null
          val mkByteBuffer = DefDef(
            NoMods,
            newTermName("mkByteBuffer"),
            List(),
            List(
                List(
                    ValDef(
                        NoMods,
                        newTermName("bytes"),
                        AppliedTypeTree(
                            Ident(newTypeName("Array")),
                            List(Ident(newTypeName("Byte")))
                        ), 
                        EmptyTree))),
            Select(
                Select(
                    Ident(newTermName("java")), 
                    newTermName("nio")),
                newTypeName("ByteBuffer")),
            Apply(
                Select(
                    Select(
                        Select(
                            Ident(newTermName("java")), 
                            newTermName("nio")),
                        newTermName("ByteBuffer")),
                    newTermName("wrap")),
                List(
                    Ident(newTermName("bytes")))))
          val mkByteBufferWithPos = atPos(tree.pos)(mkByteBuffer)

          // def getSchema = org.apache.avro.Schema.parse(_schema)
          // TODO: move the parsed schema into an object (so we don't
          // have to reparse the same schema for every class instance)
          // Note: the instance variable _schema gets generated in later
          // stages
          val getSchema = DefDef(
            NoMods,
            newTermName("getSchema"),
            List(),
            List(List()),
            Select(
                Select(
                    Select(
                        Ident(newTermName("org")), 
                        newTermName("apache")),
                    newTermName("avro")),
                newTypeName("Schema")),
            Apply(
                Select(
                    Select(
                        Select(
                            Select(
                                Ident(newTermName("org")), 
                                newTermName("apache")),
                            newTermName("avro")),
                        newTermName("Schema")),
                    newTermName("parse")),
                List(
                    Ident(newTermName("_schema"))))
            )
          val getSchemaWithPos = atPos(tree.pos)(getSchema)

          // def this() = super()
          // i dont think you can even do this in scala, but you can write an
          // AST for it :)
          // TODO: is this safe (it is possible that future versions will disallow this)?
          val ctor = DefDef(
            NoMods,
            nme.CONSTRUCTOR,
            List(),
            List(List()),
            TypeTree(),
            Block(
                List(
                    Apply(
                        Select(
                            Super("",""),
                            newTermName("<init>")),
                        List())),
                Literal(Constant(()))))
          val ctorWithPos = atPos(tree.pos.focus)(ctor)

          // class X extends org.apache.avro.specific.SpecificRecordBase
          val specificRecordBase = 
                    Select(
                        Select(
                            Select(
                                Select(
                                    Ident(newTermName("org")), 
                                    newTermName("apache")),
                                newTermName("avro")),
                            newTermName("specific")),
                        newTypeName("SpecificRecordBase"))

          // class X extends org.apache.avro.specific.SpecificRecordBase with org.apache.avro.specific.SpecificRecord
          val specificRecord = 
                    Select(
                        Select(
                            Select(
                                Select(
                                    Ident(newTermName("org")), 
                                    newTermName("apache")),
                                newTermName("avro")),
                            newTermName("specific")),
                        newTypeName("SpecificRecord"))

          val avroConversions = 
                Select(
                    Select(
                        Select(
                            Ident(newTermName("com")),
                            newTermName("googlecode")),
                        newTermName("avro")),
                    newTypeName("AvroConversions"))

	      val newImpl = treeCopy.Template(impl, List(specificRecordBase, specificRecord, avroConversions) ::: impl.parents, impl.self, List(getSchemaWithPos, mkUtf8WithPos, mkByteBufferWithPos) ::: impl.body ::: List(ctorWithPos))


	      treeCopy.ClassDef(tree, mods, name, tparams, newImpl)
      case _ => tree
    }

    override def transform(tree: Tree): Tree = {
      val t = preTransform(tree)
      super.transform(t)
    }
  }
}
