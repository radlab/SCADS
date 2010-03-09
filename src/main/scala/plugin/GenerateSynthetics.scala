package com.googlecode.avro

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
import nsc.ast.TreeDSL
import nsc.typechecker
import scala.annotation.tailrec

import org.apache.avro.Schema
import org.apache.avro.Schema.Field
import org.apache.avro.Schema.Type

import scala.collection.JavaConversions._

import scala.collection.mutable.ListBuffer

class GenerateSynthetics(plugin: ScalaAvroPlugin, val global : Global) extends PluginComponent
  with Transform
  with TypingTransformers
  with TreeDSL
{
  import global._
  import definitions._
  	  
  val runsAfter = List[String]("earlytyper")
  val phaseName = "generatesynthetics"
  def newTransformer(unit: CompilationUnit) = new ScalaAvroTransformer(unit)    

  //val utf8Class = definitions.getClass(newTypeName("org.apache.avro.util.Utf8"))


  val boxedMap = Map( 
    IntClass.tpe -> BoxedIntClass.tpe) 

  class ScalaAvroTransformer(unit: CompilationUnit) extends TypingTransformer(unit) {
    import CODE._
    import Helpers._

    private def mkDelegate(owner: Symbol, tgtMember: Symbol, tgtMethod: Symbol, pos: Position) = {
      val delegate = cloneMethod(tgtMethod, owner)
	        
      log("owner=" + This(owner))
	  
      val selectTarget = This(owner) DOT tgtMember DOT tgtMethod
      log("SelectTarget=")
      log(nodeToString(selectTarget))
	  
      val rhs : Tree =
        delegate.info match {
          case MethodType(params, _) => Apply(selectTarget, params.map(Ident(_)))
          case _ => selectTarget
        }
	    
      val delegateDef = localTyper.typed { DEF(delegate) === rhs } 
	    
      log(nodePrinters nodeToString delegateDef)
    
      delegateDef
    }
	
    private def publicMembersOf(sym:Symbol) =
      sym.tpe.members.filter(_.isPublic).filter(!_.isConstructor)
	
    private def publicMethodsOf(sym:Symbol) =
      publicMembersOf(sym).filter(_.isMethod)
  
    private def cloneMethod(prototype: Symbol, owner: Symbol) = {
      val newSym = prototype.cloneSymbol(owner)
      newSym setPos owner.pos.focus
      newSym setFlag SYNTHETICMETH
      owner.info.decls enter newSym    	
    }
      
    def generateSetterAndGetter(templ: Template, instanceVar: Symbol) = {
        val owner = instanceVar.owner 
        //var owner = currentOwner
        println(owner)
        val getterTarget = This(owner) DOT instanceVar
        //val getterTarget = Ident(instanceVar)
        //val getterTarget = LIT(15)

        //val newSym = instanceVar.cloneSymbol
        //newSym setFlag SYNTHETICMETH

        //val newSym = new MethodSymbol(owner, instanceVar.pos, newTermName("get"+instanceVar.toString))
        val newSym = owner.newMethod(owner.pos.focus, newTermName("get"+instanceVar.name.toString.trim))
        newSym setFlag SYNTHETICMETH 
        //newSym setInfo instanceVar.cloneSymbol.info
        newSym setInfo MethodType(newSym.newSyntheticValueParams(List()), instanceVar.tpe)
        owner.info.decls enter newSym 
        
        println(newSym)
        val defT = DEF(newSym) === getterTarget
        println(defT)
        // newMethod(newTermName("get" + instanceVar.toString))
        val getterDef = localTyper.typed { defT  }
        
        println(getterDef)
        getterDef
    }

    def generateDefaultCtor(templ: Template, clazz: Symbol, instanceVars: List[Symbol]) = {
        val newCtor = clazz.newConstructor(clazz.pos.focus)
        newCtor setInfo MethodType(newCtor.newSyntheticValueParams(List()), clazz.tpe)
        clazz.info.decls enter newCtor
        val values = for ((sym, i) <- instanceVars.zipWithIndex) yield {
            if (sym.tpe.typeSymbol == IntClass)
                LIT(0)
            else if (sym.tpe.typeSymbol == LongClass)
                LIT(0L)
            else if (sym.tpe.typeSymbol == FloatClass)
                LIT(0.0f)
            else if (sym.tpe.typeSymbol == DoubleClass)
                LIT(0.0d)
            else if (sym.tpe.typeSymbol == BooleanClass)
                LIT(false)
            else 
                LIT(null)
        }

        typer typed {
            DEF(newCtor) === {
                Block(
                    List(Apply(
                        Select( 
                            This(clazz),
                            newTermName("<init>")),
                        values)),
                    Literal(Constant()))
            }
        }
    }

    def generateSetMethod(templ: Template, clazz: Symbol, instanceVars: List[Symbol]) = {
        val newSym = clazz.newMethod(clazz.pos.focus, newTermName("put"))
        newSym setFlag SYNTHETICMETH 
        newSym setInfo MethodType(newSym.newSyntheticValueParams(List(IntClass.tpe, AnyClass.tpe)), UnitClass.tpe)
        clazz.info.decls enter newSym 

        //val idx = newSym ARG 0
        //idx.tpe = IntClass.tpe
        //val obj = newSym ARG 1
        //obj.tpe = AnyClass.tpe
        
        //newSym.info.decls enter idx
        //newSym.info.decls enter obj

        val default = List(DEFAULT ==> THROW(IndexOutOfBoundsExceptionClass, newSym ARG 0))

        println("-------")
        val list = clazz.info.decls.toList filter (_ hasFlag ACCESSOR)
        println(list)
        println("name list: " + list.map(_.name.toString))

        println("-----")
        println(unit.depends)
        println(unit.defined)

        println(unit.depends.map(s => (s.fullNameString,s.tpe.normalize.toString)))

        val byteBufferTpe = unit.depends.filter(i => i.fullNameString == "java.nio.ByteBuffer" && !i.isModuleClass).head.tpe
        val utf8Tpe = unit.depends.filter(i => i.fullNameString == "org.apache.avro.util.Utf8" && !i.isModuleClass).head.tpe

        println("utf8Tpe: " + utf8Tpe)

        val cases = for ((sym, i) <- instanceVars.zipWithIndex) yield {
            //val castType = boxedMap get sym.tpe getOrElse sym.tpe
            //val cast = obj AS sym.tpe
            //cast match {
            //    case TypeApply(Select(qual, _), List(targ)) =>
            //        println("qual: " + qual)
            //        println("targ: " + targ)
            //        println("qual.tpe: " + qual.tpe)
            //        println("targ.tpe: " + targ.tpe)
            //        println("---")
            //    case _ => println("no match")
            //}
            

            val accessors = list.filter(_.name.toString.trim equals (sym.name.toString.trim + "_$eq"))
            println("accessor: " + accessors)
            //println("target.class" + target.getClass)

            //val target = Assign(This(clazz) DOT sym, cast)

            val rhs = 
                if (sym.tpe.typeSymbol == StringClass) {
                    typer typed ((newSym ARG 1) AS utf8Tpe DOT newTermName("toString"))
                } else if (sym.tpe.typeSymbol == ArrayClass && sym.tpe.normalize.typeArgs.head == ByteClass.tpe) {
                    typer typed ((newSym ARG 1) AS byteBufferTpe.normalize DOT newTermName("array"))
                } else {
                    typer typed ((newSym ARG 1) AS sym.tpe)
                }
            //val target = Apply(This(clazz) DOT accessors(0), List(typedDot))
            val target = Assign(This(clazz) DOT sym, rhs)
            //target match {
            //    case Apply(fn, args) =>
            //        if (fn.symbol == Any_asInstanceOf)
            //            fn match {
            //                case TypeApply(Select(qual, _), List(targ)) =>
            //                    println("qual: " + qual)
            //                    println("targ: " + targ)
            //                    println("qual.tpe: " + qual.tpe)
            //                    println("targ.tpe: " + targ.tpe)
            //                    println("---")
            //                case _ => println("no match in type apply")
            //            }
            //        else
            //            println("not an any instance of")
            //    case _ => println("No match in target")
            //}

            CASE(LIT(i)) ==> target
        }

        println(boxedClass)
        println(cases)

        val deffer = localTyper.typed {
            DEF(newSym) === {
                (newSym ARG 0) MATCH { cases ::: default : _* }
            }   
        }   

        deffer
    }

    def string2utf8(clazz: Symbol, sym: Symbol): Tree = {
        //val utf8 = unit.depends.toList.filter(_.name.toString.trim.equals("Utf8")).head
        //val utf8 = clazz.info.decls.iterator.toList.filter(_.name.toString.trim.equals("Utf8")).head
        //println("Found Utf8?: " + utf8)

        Apply(
            //Select(New(utf8, newTermName("<init>"))),
            This(clazz) DOT newTermName("mkUtf8"),
            List(This(clazz) DOT sym))
        //NEW(utf8, This(clazz) DOT sym)
    }

    def byteArray2byteBuffer(clazz: Symbol, sym: Symbol): Tree = {
        Apply(
            This(clazz) DOT newTermName("mkByteBuffer"),
            List(This(clazz) DOT sym))
    }

    def sym2ident(clazz: Symbol, sym: Symbol): Tree = This(clazz) DOT sym AS ObjectClass.tpe

    // TODO: don't use strings as keys, find a more general solution here
    private var symMap = Map(
            StringClass.tpe.toString -> ((clazz:Symbol, sym:Symbol) => string2utf8(clazz,sym)),
            "Array[Byte]" -> ((clazz:Symbol, sym:Symbol) => byteArray2byteBuffer(clazz,sym))  
            )

    def generateGetMethod(templ: Template, clazz: Symbol, instanceVars: List[Symbol]) = {
        val newSym = clazz.newMethod(clazz.pos.focus, newTermName("get"))
        newSym setFlag SYNTHETICMETH | OVERRIDE 
        newSym setInfo MethodType(newSym.newSyntheticValueParams(List(/*Boxed*/ IntClass.tpe)), /*Any*/ObjectClass.tpe)
        clazz.info.decls enter newSym 

        //val arg = (newSym ARG 0) DOT newTermName("intValue")
        val arg = newSym ARG 0
        val default = List(DEFAULT ==> THROW(IndexOutOfBoundsExceptionClass, arg))
        println(symMap)
        val cases = for ((sym, i) <- instanceVars.zipWithIndex) yield {
            //val castType = boxedMap get sym.tpe getOrElse sym.tpe
            //val target = This(clazz) DOT sym AS castType
            //val typedSymTree = localTyper typed { Ident(sym) }
            //println("sym.tpe: " + typedSymTree.symbol.tpe.toString)
            println("sym.tpe.normalize" + sym.tpe.normalize)
            println("sym.tpe.finalResultType" + sym.tpe.finalResultType)
            CASE(LIT(i)) ==> {
                val fn = symMap get (sym.tpe.normalize.toString) getOrElse ( (c:Symbol, s:Symbol) => sym2ident(c,s) )
                fn(clazz, sym)
            }
        }

        println(boxedClass)
        println(boxedMap)
        println(cases)

        val deffer = localTyper.typed {
            DEF(newSym) === {
                arg MATCH { cases ::: default : _* }
            }   
        }   

        deffer
    }

    def generateDelegates(templ: Template, symbolToProxy: Symbol) : List[Tree] = {
      val cls = symbolToProxy.owner  //the class owning the symbol
    	
      log("proxying symbol: " + symbolToProxy)
      log("owning class: " + cls)
        
      val definedMethods = publicMembersOf(cls)
      val requiredMethods =
        publicMembersOf(symbolToProxy).filter(mem => !definedMethods.contains(mem))
    	
      log("defined methods: " + definedMethods.mkString(", "))
      log("missing methods: " + requiredMethods.mkString(", "))

      val synthetics = for (method <- requiredMethods) yield
        mkDelegate(cls, symbolToProxy, method, symbolToProxy.pos.focus)
      
      synthetics
    }

    def modifyGetSchemaMethod(name: Name, templ: Template, clazz: Symbol, instanceVars: List[Symbol]) = {
        println("name: " + name)
        println("clazz.name: " + clazz.name)
        println("clazz.owner.name: " + clazz.owner.name)
        println("clazz.owner.fullNameString: " + clazz.owner.fullNameString)
        println("clazz.fullNameString: " + clazz.fullNameString) 


        val newRecord = Schema.createRecord(clazz.name.toString, "Auto-Generated Schema", clazz.owner.fullNameString, false)

        println("StringClass.tpe.toString: " + StringClass.tpe.toString)

        val fieldList = instanceVars.map( iVar => {
            val fieldSchema = 
                if (iVar.tpe == IntClass.tpe)
                    Schema.create(Type.INT)
                else if (iVar.tpe == LongClass.tpe)
                    Schema.create(Type.LONG)
                else if (iVar.tpe == FloatClass.tpe)
                    Schema.create(Type.FLOAT)
                else if (iVar.tpe == DoubleClass.tpe)
                    Schema.create(Type.DOUBLE)
                else if (iVar.tpe == BooleanClass.tpe)
                    Schema.create(Type.BOOLEAN)
                else if (iVar.tpe.typeSymbol == StringClass)
                    Schema.create(Type.STRING)
                else if (iVar.tpe.typeSymbol == ArrayClass) {
                    println("iVar.tpe.normalize.typeArgs.head: " + iVar.tpe.normalize.typeArgs.head) 
                    if (iVar.tpe.normalize.typeArgs.head != ByteClass.tpe)
                        throw new UnsupportedOperationException("Bad Array Found: " + iVar.tpe)
                    Schema.create(Type.BYTES)
                } else 
                    throw new UnsupportedOperationException("Cannot support yet: " + iVar.tpe)

            new Field(
                iVar.name.toString.trim,
                fieldSchema,
                "Auto-Generated Field",
                null /* TODO: default values */
            )
        })
    
        val b = new ListBuffer[Field]
        b.appendAll(fieldList.iterator)
        newRecord.setFields(b)
        println("newRecord: " + newRecord.toString)

        val newSym = clazz.newValue(clazz.pos.focus, newTermName("_schema"))
        newSym setFlag PRIVATE 
        newSym setInfo ConstantType(Constant(newRecord.toString)) 
        clazz.info.decls enter newSym 

        localTyper typed {
            VAL(newSym) === LIT(newRecord.toString)
        }
    } 
   
    override def transform(tree: Tree) : Tree = {
        def isAnnotatedSym(sym: Symbol) = {	 	
            if (sym != null) {
                val testSym = if (sym.isModule) sym.moduleClass else sym
                testSym.annotations exists { _.toString == plugin.avroRecordAnnotationClass }
            } else false
        }

    def isAccessor(tree: Tree) = tree match {
        case m:MemberDef if m.mods.isAccessor => true
        case _ => false
    }

    def isInstanceVar(tree: Tree) = tree match {
        case v:ValDef => true            
        case _ => false
    }

      if (!isAnnotatedSym(tree.symbol))
          return super.transform(tree)
		   
	  val newTree = tree match {
        case cd @ ClassDef(mods, name, tparams, impl) =>

          val instanceVars = for (member <- impl.body if isInstanceVar(member)) yield { member.symbol }
          println("instanceVars: " + instanceVars)
          val newMethods = List(
                //generateDefaultCtor(impl, cd.symbol, instanceVars),
                generateGetMethod(impl, cd.symbol, instanceVars),
                generateSetMethod(impl, cd.symbol, instanceVars))

          val schemaField = modifyGetSchemaMethod(name, impl, cd.symbol, instanceVars)

          //val specificRecordBase = typer typed {
          //          Select(
          //              Select(
          //                  Select(
          //                      Select(
          //                          Ident(newTermName("org")), 
          //                      newTermName("apache")),
          //                      newTermName("avro")),
          //                  newTermName("specific")),
          //              newTypeName("SpecificRecordBase")) }

          //val specificRecord = typer typed {
          //          Select(
          //              Select(
          //                  Select(
          //                      Select(
          //                          Ident(newTermName("org")), 
          //                      newTermName("apache")),
          //                      newTermName("avro")),
          //                  newTermName("specific")),
          //              newTypeName("SpecificRecord")) }



	      val newImpl = treeCopy.Template(impl, /*List(specificRecordBase, specificRecord) ::: */impl.parents, impl.self,  List(schemaField) ::: newMethods ::: impl.body)


	      //val delegs = for (member <- impl.body if isInstanceVar(member)) yield {
	      //  //log("found annotated member: " + member)
          //  println("Found instance var: " + member) 

	      //  //generateDelegates(impl, member.symbol)
          //  //List(member)
          //  List(generateSetterAndGetter(impl, member.symbol))
	      //}
	      //val newImpl = treeCopy.Template(impl, impl.parents, impl.self, delegs.flatten ::: impl.body)

	      treeCopy.ClassDef(tree, mods, name, tparams, newImpl)
	    case _ => tree
	  }
	  super.transform(newTree)
	}    
  }
}
