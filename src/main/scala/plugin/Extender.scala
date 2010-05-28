package com.googlecode.avro
package plugin

import scala.tools.nsc.symtab.Flags
import Flags._
import scala.tools.nsc._
import scala.tools.nsc.plugins.PluginComponent
import scala.tools.nsc.transform.Transform
import scala.tools.nsc.util.{Position, NoPosition}
import scala.tools.nsc.ast.TreeDSL

trait Extender extends ScalaAvroPluginComponent
               with    Transform 
               with    TreeDSL {
  import global._
  import global.definitions._

  val runsAfter = List[String]("parser")
  override val runsRightAfter = Some("parser")
  val phaseName = "extender"

  def newTransformer(unit: CompilationUnit) = new ExtenderTransformer(unit)

  class ExtenderTransformer(val unit: CompilationUnit) extends Transformer {
    import CODE._

    def preTransform(tree: Tree): Tree = tree match {
      case cd @ ClassDef(mods, name, tparams, impl) 
        if (mods.annotations.exists {
              case Apply(Select(New(sym),_),_) => sym.toString.indexOf("AvroRecord") != -1 }) => 
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

        // check that this annotation is a case class
        if (!cd.hasFlag(Flags.CASE))
          throw new NonCaseClassException(name.toString)

        debug("Extending class: " + name.toString)

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

        def toSelectTree(s: String) = {
          if ((s eq null) || s.isEmpty)
            throw new IllegalArgumentException("Bad FQDN")
          val (car, cdr) = s.split("\\.").toList.splitAt(1)
          if (cdr isEmpty)
            throw new IllegalArgumentException("Nothing to select: " + s)
          else
            /* TODO: not sure why the [Tree] is needed for the typer */
            cdr.zipWithIndex.foldLeft[Tree](Ident(newTermName(car.head)))((tree, tuple) => {
              val (sel, idx) = tuple
              Select(tree, if (idx == cdr.length - 1) newTypeName(sel) else newTermName(sel))
            })
        }

        // class X extends org.apache.avro.specific.SpecificRecordBase
        val specificRecordBase = toSelectTree("org.apache.avro.specific.SpecificRecordBase")

        // class X extends org.apache.avro.specific.SpecificRecordBase with
        //   com.googlecode.avro.runtime.ScalaSpecificRecord
        val scalaSpecificRecord = toSelectTree("com.googlecode.avro.runtime.ScalaSpecificRecord")

        val avroConversions = toSelectTree("com.googlecode.avro.runtime.AvroConversions")

        val newImpl = treeCopy.Template(impl, List(specificRecordBase, scalaSpecificRecord, avroConversions) ::: impl.parents, impl.self, impl.body ::: List(ctorWithPos))
        treeCopy.ClassDef(tree, mods, name, tparams, newImpl)
      case _ => tree
    }

    override def transform(tree: Tree): Tree = {
      val t = preTransform(tree)
      super.transform(t)
    }
  }
}
