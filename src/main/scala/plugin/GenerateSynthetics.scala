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
import nsc.util.NoPosition
import nsc.ast.TreeDSL
import nsc.typechecker
import scala.annotation.tailrec

import org.apache.avro.Schema
import org.apache.avro.Schema.Field
import org.apache.avro.Schema.{Type => AvroType}

import scala.collection.JavaConversions._

import scala.collection.mutable.{HashSet,ListBuffer}

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

  class ScalaAvroTransformer(unit: CompilationUnit) extends TypingTransformer(unit) {
    import CODE._
    import Helpers._

    val primitiveClasses = Map(
        IntClass     -> Schema.create(AvroType.INT),
        FloatClass   -> Schema.create(AvroType.FLOAT),
        LongClass    -> Schema.create(AvroType.LONG),
        DoubleClass  -> Schema.create(AvroType.DOUBLE),
        BooleanClass -> Schema.create(AvroType.BOOLEAN),
        StringClass  -> Schema.create(AvroType.STRING)
    )

    def generateSetMethod(templ: Template, clazz: Symbol, instanceVars: List[Symbol]) = {
        val newSym = clazz.newMethod(clazz.pos.focus, newTermName("put"))
        newSym setFlag SYNTHETICMETH 
        newSym setInfo MethodType(newSym.newSyntheticValueParams(List(IntClass.tpe, AnyClass.tpe)), UnitClass.tpe)
        clazz.info.decls enter newSym 

        val default = List(DEFAULT ==> THROW(IndexOutOfBoundsExceptionClass, newSym ARG 0))

        val list = clazz.info.decls.toList filter (_ hasFlag ACCESSOR)

        // TODO: find a better way to get a handle to these types
        val byteBufferTpe = unit.depends.filter(i => i.fullNameString == "java.nio.ByteBuffer" && !i.isModuleClass).head.tpe
        val utf8Tpe = unit.depends.filter(i => i.fullNameString == "org.apache.avro.util.Utf8" && !i.isModuleClass).head.tpe

        println("utf8Tpe: " + utf8Tpe)

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

    def string2utf8(clazz: Symbol, sym: Symbol): Tree = {
        Apply(
            This(clazz) DOT newTermName("mkUtf8"),
            List(This(clazz) DOT sym))
    }

    def byteArray2byteBuffer(clazz: Symbol, sym: Symbol): Tree = {
        Apply(
            This(clazz) DOT newTermName("mkByteBuffer"),
            List(This(clazz) DOT sym))
    }

    def listToGenericArray(clazz: Symbol, sym: Symbol): Tree = {
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

    def sym2ident(clazz: Symbol, sym: Symbol): Tree = This(clazz) DOT sym AS ObjectClass.tpe

    // TODO: don't use strings as keys, find a more general solution here
    private var symMap = Map(
            StringClass -> ((clazz:Symbol, sym:Symbol) => string2utf8(clazz,sym)),
            ArrayClass  -> ((clazz:Symbol, sym:Symbol) => 
                if (sym.tpe.typeArgs.head.typeSymbol == ByteClass)
                    byteArray2byteBuffer(clazz,sym)
                else
                    throw new UnsupportedOperationException("Cannot handle this right now")
                ),
            ListClass   -> ((clazz:Symbol, sym:Symbol) =>  listToGenericArray(clazz,sym))
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
                val fn = symMap get (sym.tpe.typeSymbol) getOrElse ( (c:Symbol, s:Symbol) => sym2ident(c,s) )
                fn(clazz, sym)
            }
        }

        println(boxedClass)
        println(cases)

        val deffer = localTyper.typed {
            DEF(newSym) === {
                arg MATCH { cases ::: default : _* }
            }   
        }   

        deffer
    }

    def createSchema(sym: Symbol): Schema = {
        println("createSchema() called with sym: " + sym)
        if (primitiveClasses.get(sym.tpe.typeSymbol).isDefined)
            primitiveClasses.get(sym.tpe.typeSymbol).get
        else if (sym.tpe.typeSymbol == ArrayClass) {
            println("sym.tpe.normalize.typeArgs.head: " + sym.tpe.normalize.typeArgs.head) 
            if (sym.tpe.normalize.typeArgs.head != ByteClass.tpe)
                throw new UnsupportedOperationException("Bad Array Found: " + sym.tpe)
            Schema.create(AvroType.BYTES)
        } else if (sym.tpe.typeSymbol == ListClass) {
            val listParam = sym.tpe.normalize.typeArgs.head 
            Schema.createArray(createSchema(listParam.typeSymbol))
        } else if (plugin.state.recordClassSchemas.get(sym.tpe.normalize.toString).isDefined) {
            plugin.state.recordClassSchemas.get(sym.tpe.normalize.toString).getOrElse(throw new IllegalStateException("should not be null"))
        } else if (plugin.state.unions.contains(sym.tpe.normalize.toString)) {
            val unionSchemas = plugin.state.unions.get(sym.tpe.normalize.toString).get.toList.map( u => {
                plugin.state.recordClassSchemas.get(u).get 
            })
            println("unionSchemas: " + unionSchemas)
            val listBuffer = new ListBuffer[Schema]
            listBuffer ++= unionSchemas
            Schema.createUnion(listBuffer)
        } else 
            throw new UnsupportedOperationException("Cannot support yet: " + sym.tpe)
    }

    def createSchemaFieldVal(name: Name, templ: Template, clazz: Symbol, instanceVars: List[Symbol]) = {
        println("name: " + name)
        println("clazz.name: " + clazz.name)
        println("clazz.owner.name: " + clazz.owner.name)
        println("clazz.owner.fullNameString: " + clazz.owner.fullNameString)
        println("clazz.fullNameString: " + clazz.fullNameString) 

        val newRecord = Schema.createRecord(clazz.name.toString, "Auto-Generated Schema", clazz.owner.fullNameString, false)

        val fieldList = instanceVars.map( iVar => {
            val fieldSchema = createSchema(iVar)
            println("fieldSchema: " + fieldSchema)
            new Field(
                iVar.name.toString.trim,
                fieldSchema,
                "Auto-Generated Field",
                null /* TODO: default values */
            )
        })
    
        val b = new ListBuffer[Field]
        b ++= fieldList
        newRecord.setFields(b)
        println("newRecord: " + newRecord.toString)
        //assert( plugin.state.contains(clazz.fullNameString) )
        plugin.state.recordClassSchemas += clazz.fullNameString -> newRecord

        val newSym = clazz.newValue(clazz.pos.focus, newTermName("_schema"))
        newSym setFlag PRIVATE 
        newSym setInfo ConstantType(Constant(newRecord.toString)) 
        clazz.info.decls enter newSym 

        localTyper typed {
            VAL(newSym) === LIT(newRecord.toString)
        }
    } 
   
    override def transform(tree: Tree) : Tree = {
        def isAnnotatedRecordSym(sym: Symbol) = {	 	
            if (sym != null) {
                val testSym = if (sym.isModule) sym.moduleClass else sym
                println("testSym: " + testSym)
                println("testSym.annotations: " + testSym.annotations)
                testSym.annotations exists { _.toString == plugin.avroRecordAnnotationClass }
            } else false
        }

        def isAnnotatedUnionSym(sym: Symbol) = {	 	
            if (sym != null) {
                val testSym = if (sym.isModule) sym.moduleClass else sym
                testSym.annotations exists { _.toString == plugin.avroUnionAnnotationClass }
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
		   
	  val newTree = tree match {
        case pd @ PackageDef(pid, stats) =>
            println("PackageDef: " + pd.name)

            // find all the avro record class defs
            val avroClassDefs = stats.filter { s =>
                s match {
                    case ClassDef(_,_,_,_) => isAnnotatedRecordSym(s.symbol) 
                    case _ => false
                }
            }

            println("avroClassDefs: " + avroClassDefs)

            // find all the union defs
            val avroUnionDefs = stats.filter { s =>
                s match {
                    case ClassDef(_,_,_,_) => isAnnotatedUnionSym(s.symbol) 
                    case _ => false
                }
            }

            // make sure that each union def is sealed, so that we *can* guarantee that 
            // we can compute closure
            if (!avroUnionDefs.filter( !_.asInstanceOf[ClassDef].mods.isSealed ).isEmpty) {
                throw new UnsupportedOperationException("all union traits must be sealed")
            }

            // add all union defs to union set 
            avroUnionDefs.foreach( ud => plugin.state.unions += ud.symbol.fullNameString -> new HashSet[String] )

            // compute closure for each union
            avroClassDefs.foreach( cd => {
                val cdCast = cd.asInstanceOf[ClassDef]
                val parents = cdCast.impl.parents
                println("parents: " + parents.map( _.symbol.fullNameString ))
                val unionDefParents = parents.filter( p => plugin.state.unions.contains(p.symbol.fullNameString) )
                unionDefParents.foreach( p => 
                    plugin.state.unions.get(p.symbol.fullNameString).get.add(cdCast.symbol.fullNameString) )
            })

            println("plugin.state.unions: " + plugin.state.unions)

            // Create a topological sorting of the class defs based on their 
            // schema dependencies + unions. If we find that the dependency graph
            // contains a cycle, we'll throw an exception because avro
            // does not let you do this (try it, it doesn't work)

            // TODO: use something better than strings here for class names
            // (like the class symbol or type)
            val dependencyGraph = new DirectedGraph[String]

            avroClassDefs.foreach( classDef => dependencyGraph.add(classDef.symbol.fullNameString) )
            avroClassDefs.foreach( classDef => plugin.state.recordClassSchemas += (classDef.symbol.fullNameString -> null) )

            def containsUnionType(sym: Symbol):Boolean = {
                if (sym.tpe.typeSymbol == ListClass)
                    containsUnionType(sym.tpe.typeArgs.head.typeSymbol)
                else
                    plugin.state.unions.contains(sym.tpe.normalize.toString)
            }

            def getUnionType(sym: Symbol): HashSet[String] = {
                if (sym.tpe.typeSymbol == ListClass)
                    getUnionType(sym.tpe.typeArgs.head.typeSymbol)
                else
                    plugin.state.unions.get(sym.tpe.normalize.toString).get
            }

            // for each class clz1
            // 1) find the union defs of that class
            // 2) for each u of those union defs
            //   a) for each clz2 in the closure of u
            //     i) add (clz1 -> clz2) to the dep graph
            avroClassDefs.foreach( classDef => {
                val unionInstanceVars = classDef.asInstanceOf[ClassDef].impl.body.filter( iv => 
                    isInstanceVar(iv) && containsUnionType(iv.symbol) ).map( iv =>
                        getUnionType(iv.symbol))
                unionInstanceVars.foreach(l => l.foreach( clz2 => {
                    dependencyGraph.addEdge(classDef.symbol.fullNameString, clz2)
                }))
            })

            println("DepGraph after unions: " + dependencyGraph)

            def containsRecordType(sym: Symbol):Boolean = {
                if (sym.tpe.typeSymbol == ListClass)
                    containsRecordType(sym.tpe.typeArgs.head.typeSymbol)
                else
                    plugin.state.recordClassSchemas.contains(sym.tpe.normalize.toString)
            }

            def getRecordType(sym: Symbol):Symbol = {
                if (sym.tpe.typeSymbol == ListClass)
                    getRecordType(sym.tpe.typeArgs.head.typeSymbol)
                else sym
            }
            

            avroClassDefs.foreach( classDef => {
                println("avroClassDef: " + classDef)
                val instanceVars = classDef.asInstanceOf[ClassDef].impl.body.filter { isInstanceVar(_) }
                println("instanceVars: " + instanceVars)
                val avroInstanceVars = instanceVars.filter( iv => containsRecordType(iv.symbol) ).map( iv => getRecordType(iv.symbol) )
                println("avroInstanceVars (Records): " + avroInstanceVars)
                avroInstanceVars.foreach( ivSym => {
                    if (!dependencyGraph.containsEdge(classDef.symbol.fullNameString, ivSym.tpe.normalize.toString))
                        dependencyGraph.addEdge(classDef.symbol.fullNameString, ivSym.tpe.normalize.toString) 
                })
            })

            println("DepGraph after classes: " + dependencyGraph)
            if (dependencyGraph.hasCycle)
                throw new IllegalStateException("Oops, your records have a cyclic dependency. Please resolve")
            val tSort = dependencyGraph.topologicalSort
            println("TSort: " + tSort)

            // TODO: this isn't very efficient
            val sortedAvroClassDefs = tSort.map( className => avroClassDefs.filter( classDef => classDef.symbol.fullNameString == className ).head )

            val newAvroStats = for (m <- sortedAvroClassDefs) yield {
                m match {
                    case cd @ ClassDef(mods, name, tparams, impl) =>
                        println("ClassDef: " + cd.name)
                        if (!isAnnotatedRecordSym(m.symbol)) {
                            throw new IllegalStateException("should never happen")
                        } else {
                            val instanceVars = for (member <- impl.body if isInstanceVar(member)) yield { member.symbol }
                            println("instanceVars: " + instanceVars)
                            val newMethods = List(
                                    generateGetMethod(impl, cd.symbol, instanceVars),
                                    generateSetMethod(impl, cd.symbol, instanceVars))

                            val schemaField = createSchemaFieldVal(name, impl, cd.symbol, instanceVars)
                            val newImpl = treeCopy.Template(impl, impl.parents, impl.self,  List(schemaField) ::: newMethods ::: impl.body)
                            treeCopy.ClassDef(m, mods, name, tparams, newImpl)
                        }
                }
            }

            var newPackageStats = for (m <- stats) yield {
                m match {
                    case cd @ ClassDef(mods, name, tparams, impl) =>
                        if (!isAnnotatedRecordSym(m.symbol)) {
                            m
                        } else {
                            newAvroStats.filter(cd => m.symbol.fullNameString == cd.symbol.fullNameString).head
                        }
                    case _ => m
                }
            }

            // TODO: move this to another method
            // new module defs 
            val newModuleDefs = newPackageStats.
                filter( s => 
                    s.isInstanceOf[ModuleDef] && 
                    plugin.state.recordClassSchemas.contains(s.symbol.fullNameString) ).
                map( _.asInstanceOf[ModuleDef] ).
                map ( md => {
                    val mods = md.mods
                    val name = md.name
                    val impl = md.impl

                    val schema = plugin.state.recordClassSchemas.get(md.symbol.fullNameString).get

                    val nonSchemaMbrs = impl.body.filterNot( mbr => mbr.isInstanceOf[ValDef] && mbr.asInstanceOf[ValDef].name.toString == "schema " )
                    val schemaMbr = impl.body.filter( mbr => mbr.isInstanceOf[ValDef] && mbr.asInstanceOf[ValDef].name.toString == "schema " ).head /* the extra space is important */
                    val newSchemaMbr = schemaMbr match {
                        case ValDef(mods, name, tpt, Apply(a0, List(Literal(Constant(_))))) =>
                            treeCopy.ValDef(schemaMbr, mods, name, tpt, localTyper typed { Apply(a0, List(Literal(Constant(schema.toString)))) } )
                        case _ =>
                            throw new IllegalStateException("should have schema value");
                    }
                    println("newSchemaMbr: " + newSchemaMbr)
                    //val newSym = md.symbol.moduleClass.newValue(md.symbol.pos.focus, newTermName("schema "))
                    //val schemaTpe = unit.depends.filter(i => i.fullNameString == "org.apache.avro.Schema" && !i.isModuleClass).head.tpe
                    //newSym setFlag PRIVATE | LOCAL
                    //newSym setInfo schemaTpe 
                    //md.symbol.info.decls enter newSym

                    //val rhs = 
                    //    Apply(
                    //        Select(
                    //            Select(
                    //                Select(
                    //                    Select(
                    //                        Ident(newTermName("org")), 
                    //                        newTermName("apache")),
                    //                    newTermName("avro")),
                    //                newTermName("Schema")),
                    //            newTermName("parse")),
                    //        List(
                    //            Literal(Constant(schema.toString))))
                    //val schemaVal   = localTyper typed { VAL(newSym) === rhs }

                    //val newMeth = md.symbol.moduleClass.newMethod(md.symbol.pos.focus, newTermName("schema"))
                    //newMeth.resetFlags
                    //newMeth setFlag METHOD | STABLE | ACCESSOR
                    //newMeth setInfo MethodType(newMeth.newSyntheticValueParams(List()), schemaTpe)
                    //md.symbol.info.decls enter newMeth 

                    //val schemaMeth = /*localTyper typed {*/ DEF(newMeth) === (This(md.symbol.moduleClass) DOT newTermName("schema ")) /*}*/
                    val newImpl = treeCopy.Template( impl, impl.parents, impl.self, nonSchemaMbrs ::: List(newSchemaMbr) /*impl.body*/ /*::: List(schemaMeth, schemaVal) */)
                    treeCopy.ModuleDef( md, mods, name, newImpl ) 
                })

            newPackageStats = for (m <- newPackageStats) yield {
                m match {
                    case md @ ModuleDef(mods, name, impl) =>
                        val replacement = newModuleDefs.filter(
                            _.symbol.fullNameString == md.symbol.fullNameString)
                        if (replacement.isEmpty)
                            m
                        else
                            replacement.head
                    case _ => m
                }
            }

            treeCopy.PackageDef(pd, pid, newPackageStats)
        case _ => tree
	  }
	  super.transform(newTree)
	}    
  }
}
