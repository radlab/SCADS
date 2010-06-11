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
import nsc.symtab.Flags
import nsc.symtab.Flags._
import nsc.util.Position
import nsc.util.NoPosition
import nsc.ast.TreeDSL
import nsc.typechecker
import scala.annotation.tailrec

trait Extender extends ScalaAvroPluginComponent
               with    Transform 
               with    InfoTransform
               with    TypingTransformers
               with    TreeDSL {
  import global._
  import global.definitions._

  val runsAfter = List[String]("typer")
  override val runsRightAfter = Some("typer")
  val phaseName = "extender"

  def newTransformer(unit: CompilationUnit) = new ExtenderTransformer(unit)

  override def transformInfo(sym: Symbol, tpe: Type): Type = tpe match {
    case ClassInfoType(parents, decls, clazz) if (!clazz.isPackageClass && clazz.tpe.parents.contains(avroRecordTrait.tpe)) =>
      // 1) warn if current parent is not java.lang.Object AND if it is not a
      // subtype of SpecificRecordBase
      val (car, cdr) = clazz.tpe.parents.splitAt(1)
      if (car.head != ObjectClass.tpe && !(car.head <:< SpecificRecordBaseClass.tpe))
        warn("Replacing inheritance of non specific record base type")
      ClassInfoType(List(SpecificRecordBaseClass.tpe, ScalaSpecificRecord.tpe, AvroConversions.tpe) ::: cdr, decls, clazz)
    case _ => tpe
  }

  class ExtenderTransformer(unit: CompilationUnit) extends TypingTransformer(unit) {
    import CODE._

    private val DefaultValues = Map(
      IntClass -> ZERO,
      LongClass -> ZERO,
      FloatClass -> ZERO,
      DoubleClass -> ZERO,
      BooleanClass -> FALSE)

    private def preTransform(tree: Tree): Tree = tree match {
      case cd @ ClassDef(mods, name, tparams, impl) 
        if (cd.symbol.tpe.parents.contains(avroRecordTrait.tpe)) =>

        // check that this annotation is a case class
        if (!cd.hasFlag(Flags.CASE))
          throw new NonCaseClassException(name.toString)

        debug("Extending class: " + name.toString)

        // def this() = super()
        // i dont think you can even do this in scala, but you can write an
        // AST for it :)
        // TODO: is this safe (it is possible that future versions will disallow this)?
        //val ctor = localTyper typed { DefDef(
        //  NoMods,
        //  nme.CONSTRUCTOR,
        //  List(),
        //  List(List()),
        //  TypeTree(),
        //  Block(
        //      List(
        //          Apply(
        //              Select(
        //                  Super("",""),
        //                  newTermName("<init>")),
        //              List())),
        //      Literal(Constant(())))) }

        def isCtor(tree: Tree): Boolean = {
          (tree.symbol ne null) && tree.symbol.name == nme.CONSTRUCTOR
        }
        val ctors = for (member <- impl.body if isCtor(member)) yield { member.symbol }
        assert (!ctors.isEmpty)

        val pos = ctors.last.pos
        val ctorSym = cd.symbol.newConstructor(pos.withPoint(pos.point + 1))
        // TODO: look for defult ctor 
        ctorSym setFlag METHOD
        ctorSym setInfo MethodType(ctorSym.newSyntheticValueParams(List()), cd.symbol.tpe)
        cd.symbol.info.decls enter ctorSym

        val instanceVars = for (member <- impl.body if isValDef(member)) yield { member.symbol }

        val ctor = localTyper typed {
          DEF(ctorSym) === Block(List(
            Apply(
              This(cd.symbol) DOT nme.CONSTRUCTOR,
              instanceVars.map(v => DefaultValues.get(v.tpe.typeSymbol).getOrElse(LIT(null)) 
            ))),
            Literal(Constant(())))
        }

        def toTypedSelectTree(s: String): Tree = {
          if ((s eq null) || s.isEmpty)
            throw new IllegalArgumentException("Bad FQDN")
          val (car, cdr) = s.split("\\.").toList.splitAt(1)
          if (cdr isEmpty)
            throw new IllegalArgumentException("Nothing to select: " + s)
          else {
            val sym = definitions.getModule(car.head)
            val first = (car.head, Ident(newTermName(car.head)) setSymbol sym setType sym.tpe)
            cdr.zipWithIndex.foldLeft[(String,Tree)](first)((tuple1, tuple2) => {
              val (name, tree) = tuple1
              val (sel, idx) = tuple2
              val newName = name + "." + sel
              val sym = 
                if (idx == cdr.length - 1)
                  definitions.getClass(newName)
                else 
                  definitions.getModule(newName)
              (newName, Select(tree, if (idx == cdr.length - 1) newTypeName(sel) else newTermName(sel)) setSymbol sym setType sym.tpe)
            })._2
          }
        }

        val specificRecordBase = toTypedSelectTree("org.apache.avro.specific.SpecificRecordBase")

        val scalaSpecificRecord = toTypedSelectTree("com.googlecode.avro.runtime.ScalaSpecificRecord")

        val avroConversions = toTypedSelectTree("com.googlecode.avro.runtime.AvroConversions")

        val (car, cdr) = impl.parents.splitAt(1)
        val newImpl = treeCopy.Template(impl, List(specificRecordBase, scalaSpecificRecord, avroConversions) ::: cdr, impl.self, impl.body ::: List(ctor))
        treeCopy.ClassDef(tree, mods, name, tparams, newImpl)
      case _ => tree
    }

    override def transform(tree: Tree): Tree = {
      val t = preTransform(tree)
      super.transform(t)
    }
  }
}
