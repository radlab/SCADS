package com.googlecode.avro

import scala.tools.nsc.symtab.Flags._
import scala.tools.nsc._
import scala.tools.nsc.plugins.PluginComponent
import scala.tools.nsc.transform.Transform
import scala.tools.nsc.util.{Position, NoPosition}
import scala.tools.nsc.ast.TreeDSL
// import scala.tools.nsc.transform.TypingTransformers

/** This class implements a plugin component using tree transformers. If
 *  a <code>Typer</code> is needed during transformation, the component
 *  should mix in <code>TypingTransformers</code>. This provides a local
 *  variable <code>localTyper: Typer</code> that is always updated to
 *  the current context.
 *
 *  @todo Adapt the name of this class to the plugin, and implement it.
 */
class InitialTransformComponent(plugin: ScalaAvroPlugin, val global: Global) extends PluginComponent
                                                     // with TypingTransformers
                                                     with Transform 
                                                     with TreeDSL {
  import global._
  import global.definitions._

  val runsAfter = List[String]("parser")
  override val runsRightAfter = Some("parser")
  /** The phase name of the compiler plugin
   *  @todo Adapt to specific plugin.
   */
  val phaseName = "initialtransform"

  def newTransformer(unit: CompilationUnit) = new InitialTransformer

  /** The tree transformer that implements the behavior of this
   *  component. Change the superclass to <code>TypingTransformer</code>
   *  to make a local typechecker <code>localTyper</code> available.
   *
   *  @todo Implement.
   */
  class InitialTransformer extends /*Typing*/ Transformer {
    import CODE._

        def isAnnotatedSym(sym: Symbol) = {	 	
            if (sym != null) {
                val testSym = if (sym.isModule) sym.moduleClass else sym
                testSym.annotations exists { _.toString == plugin.avroRecordAnnotationClass }
            } else false
        }


    /** When using <code>preTransform</code>, each node is
     *  visited before its children.
     */
    def preTransform(tree: Tree): Tree = tree match {
      case dd @ DocDef(comment, definition) => 
        println("Found docdef: " + comment)
        tree
      case cd @ ClassDef(mods, name, tparams, impl) =>
          println("mods: " + mods)
          println("tparams: " + tparams)
          println("impl.self: " + impl.self)
          println("impl.parents: " + impl.parents)
          println("impl.body: " + impl.body)
        //if (!isAnnotatedSym(tree.symbol))
        //    return super.transform(tree)
          
          if (!mods.annotations.exists( a => {
            a match {
                case Apply(fun, args) =>
                    fun match {
                        case Select(qual, sym) => 
                            qual match {
                                case New(sym) => 
                                    println("sym: " + sym)
                                    sym.toString == plugin.avroRecordAnnotationClass 
                                case _ => false
                            }
                        case _ => false
                    }
                case _ => false
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

          //val mkUtf8 = cd.newMethod(cd.pos.focus, newTermName("mkUtf8"))
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
            /*Literal(Constant(null))*/
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

	      val newImpl = treeCopy.Template(impl, List(specificRecordBase, specificRecord) ::: impl.parents, impl.self, List(getSchemaWithPos, mkUtf8WithPos, mkByteBufferWithPos) ::: impl.body)


	      treeCopy.ClassDef(tree, mods, name, tparams, newImpl)
      //case ModuleDef(mods, name, impl) =>
      //  println("found module: " + name)
      //  tree
      case _ => tree
    }

    override def transform(tree: Tree): Tree = {
      val t = preTransform(tree)
      super.transform(t)
    }
  }
}
