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

import scala.collection.mutable.{HashSet,ListBuffer}

import org.apache.avro.Schema
import org.apache.avro.Schema.Field
import org.apache.avro.Schema.{Type => AvroType}

import java.util.{Arrays => JArrays}

/** TODO: This phase isn't a transformer, refactor into a traverser */
trait SchemaGen extends ScalaAvroPluginComponent
                with    Transform
                with    TypingTransformers
                with    TreeDSL {
  import global._
  import definitions._
  	  
  val runsAfter = List[String]("schemacreate")
  override val runsRightAfter = Some("schemacreate")
  val phaseName = "schemagen"
  def newTransformer(unit: CompilationUnit) = new SchemaGenTransformer(unit)    

  class SchemaGenTransformer(unit: CompilationUnit) extends TypingTransformer(unit) {
    import CODE._

    private val primitiveClasses = Map(
      /** Primitives in the Scala and Avro sense */
      IntClass     -> Schema.create(AvroType.INT),
      FloatClass   -> Schema.create(AvroType.FLOAT),
      LongClass    -> Schema.create(AvroType.LONG),
      DoubleClass  -> Schema.create(AvroType.DOUBLE),
      BooleanClass -> Schema.create(AvroType.BOOLEAN),
      StringClass  -> Schema.create(AvroType.STRING),
      NullClass    -> Schema.create(AvroType.NULL),

      /** Types which are primitives in Scala, but have no exact match in Avro */
      ShortClass   -> Schema.create(AvroType.INT),
      ByteClass    -> Schema.create(AvroType.INT),
      CharClass    -> Schema.create(AvroType.INT),

      /** Primitives in the Avro sense */
      byteBufferClass -> Schema.create(AvroType.BYTES),
      utf8Class       -> Schema.create(AvroType.STRING)
    )

    private def createSchema(tpe: Type): Schema = {
      if (primitiveClasses.contains(tpe.typeSymbol)) {
        primitiveClasses(tpe.typeSymbol)
      } else if (tpe.typeSymbol == ArrayClass) {
        if (tpe.normalize.typeArgs.head != ByteClass.tpe)
          throw new UnsupportedOperationException("Bad Array Found: " + tpe + ". Use scala collections for lists")
        createSchema(byteBufferClass.tpe)
      } else if (tpe.typeSymbol.isSubClass(TraversableClass)) {
        tpe.typeArgs.size match {
          case 1 =>
            Schema.createArray(createSchema(tpe.typeArgs.head))
          case 2 =>
            if (tpe.typeArgs.head.typeSymbol != StringClass && 
                tpe.typeArgs.head.typeSymbol != utf8Class)
              throw new UnsupportedOperationException("Avro maps require string/utf8 keys")
            Schema.createMap(createSchema(tpe.typeArgs.tail.head))
          case i =>
            throw new AssertionError("Oops, %d type args found".format(i))
        }
      } else if (tpe.typeSymbol == OptionClass) {
        val listParam = tpe.typeArgs.head
        if (listParam.typeSymbol == OptionClass)
          throw new UnsupportedOperationException("Implementation limitation: Cannot nest option types")
        if (isUnion(listParam.typeSymbol)) {
          // special case when you do Option[A], where A is an AvroUnion
          val innerSchemas = createSchema(listParam).getTypes.toArray(Array[Schema]())
          Schema.createUnion(JArrays.asList(
            (Array(createSchema(NullClass.tpe)) ++ innerSchemas):_*))
        } else
          Schema.createUnion(JArrays.asList(
            Array(createSchema(NullClass.tpe), createSchema(listParam)):_*))
      } else if (isRecord(tpe.typeSymbol)) { 
        retrieveRecordSchema(tpe.typeSymbol).get 
      } else if (isExternalRecord(tpe.typeSymbol)) {
        retrieveExternalRecordSchema(tpe.typeSymbol)
      } else if (isUnion(tpe.typeSymbol)) {
        getOrCreateUnionSchema(tpe.typeSymbol, Schema.createUnion(JArrays.asList(
          retrieveUnionRecords(tpe.typeSymbol).
          map(_.tpe).
          map(t => createSchema(t)).toArray:_*)))
      } else throw new UnsupportedOperationException("Cannot support yet: " + tpe)
    }

    override def transform(tree: Tree) : Tree = {
      val newTree = tree match {
        case cd @ ClassDef(mods, name, tparams, impl) if (cd.symbol.tpe.parents.contains(avroRecordTrait.tpe)) =>

          val instanceVars = for (member <- impl.body if isValDef(member)) yield { member.symbol }

          /** Check to see if any of the members are immutable */
          val instanceVals = instanceVars.filter(v => !isVarSym(v))
          if (!instanceVals.isEmpty) {
            throw new ImmutableFieldException(instanceVals.mkString(", "))
          }

          debug("instance vars for class " + cd.symbol.fullName)
          debug(instanceVars)

          val fields = instanceVars.map(iVar => 
            new Field(iVar.name.toString.trim, 
                      createSchema(iVar.tpe),
                      "Auto-Generated Field",
                      null))
          
          retrieveRecordSchema(cd.symbol).get.setFields(JArrays.asList(fields.toArray:_*))

          cd
        case _ => tree
      }
      super.transform(newTree)
    }    
  }
}
