package edu.berkeley.cs.scads.test

import org.specs._
import org.specs.runner.JUnit4

import edu.berkeley.cs.scads.Compiler
import edu.berkeley.cs.scads.model.{Entity,BooleanField,IntegerField,Field,StringField,ValueHoldingField,CompositeField,Environment}

import edu.berkeley.cs.scads.model.{TrivialExecutor,TrivialSession}
//import edu.berkeley.cs.scads.TestCluster

import java.io.File
import java.net.URLClassLoader
import java.util.ResourceBundle

import java.lang.reflect.Method
import java.lang.reflect.InvocationTargetException

import org.apache.log4j.Logger

import scala.xml.{Node,NodeSeq}

case class BadDataXMLInputException(m:String) extends Exception

abstract class ScadsLangSpec extends SpecificationWithJUnit("SCADS Lang Specification") {
    val llogger = Logger.getLogger("scads.test")
    val specName: String
    val specFile: String
    val classNameMap: Map[String,Array[String]]
    val dataXMLFile: String
    val queries: Map[String,Array[String]]
    val queriesXMLFile: String
    //val metadataMap: Map[String,List[String]] = createMetadataMap()

    implicit val env = new Environment
    env.placement = new TestCluster
    env.session = new TrivialSession
    env.executor = new TrivialExecutor

    val specSource = getSourceFromFile(specFile)
    val baseDir = new File("target/generated")
    val classfilesDir = new File(baseDir, "classfiles")
    val jarFile = new File(baseDir, "spec.jar")

    val classLoader = new URLClassLoader(Array(jarFile.toURI.toURL))

    val dataXMLFileObj = new File(dataXMLFile)
    val dataNode = scala.xml.XML.loadFile(dataXMLFile)

    val pkMetadata = primaryKeyMetadata()
    val attrMetadata = attributesMetadata()

    // maps ( className -> list of ( attrName, fieldClass ) )
    def primaryKeyMetadata(): Map[String,List[Tuple2[String,Field]]] = {
        var rtn = Map[String,List[Tuple2[String,Field]]]()
        (dataNode \\ "metadata").foreach((node) => {
            val className = (node \ "@class").text
            val pks = (node\"primary-key"\"attribute").map( (node) => {
                processAttributeMetadataNode(rtn, node)} )
            if ( !pks.isEmpty ) rtn += (className -> pks.toList)
        })
        rtn
    }

    def processAttributeMetadataNode(rtn: Map[String,List[Tuple2[String,Field]]], node:Node):Tuple2[String,Field] = {
        val name = (node\"@name").text
        val text = (node\"@type").text
        val ref  = (node\"@ref").text
        if ( !text.isEmpty ) {
            (name, getFieldClass(text))
        } else if ( !ref.isEmpty ) {
            if ( rtn.keySet.contains(ref) ) {
                (name, CompositeField( rtn(ref).map(_._2).toArray : _* ))
            } else {
                throw new BadDataXMLInputException("No such ref " + ref)
            }
        } else {
            throw new BadDataXMLInputException("need either a type or a ref")
        }
    }

    def attributesMetadata(): Map[String,List[Tuple2[String,Field]]] = {
        var rtn = Map[String,List[Tuple2[String,Field]]]()
        (dataNode \\ "metadata").foreach((node) => {
            val className = (node \ "@class").text
            val pks = ((node\"attributes"\"attribute")++(node\"foreign-keys"\"attribute")).map( (node) => {
                processAttributeMetadataNode(pkMetadata, node)} )
            if ( !pks.isEmpty ) rtn += (className -> pks.toList)
        })
        rtn
    }

    //def getPKFieldFor(entity:Entity,attrs:List[String]):Field = {
    //    val className = entity.getClass.getName
    //    CompositeField(attrs.map(entity.attributes(_)).toArray : _* )
    //}

    //def getEmptyPKFieldFor(entity:Entity,attrs:List[String]):Field = {
    //    val entClazz = entity.getClass
    //    val entConstructor = entClazz.getConstructor(env.getClass)
    //    val ent = entConstructor.newInstance(env)
    //    getPKFieldFor(ent.asInstanceOf[Entity],attrs)
    //}

    def getSourceFromFile(file: String): String = {
		scala.io.Source.fromFile(file).getLines.foldLeft(new StringBuilder)((x: StringBuilder, y: String) => x.append(y)).toString
    }

    def makeNecessaryDirs() = {
        baseDir.mkdirs
        classfilesDir.mkdirs
    }

    def cleanup() = {
        classfilesDir.listFiles.foreach(_.delete)
        jarFile.delete
    }

    def loadClass(name: String): Class[Any] = {
        classLoader.loadClass(name).asInstanceOf[Class[Any]]
    }

    def getQueryMethod(className: String, queryName: String): Method = {
        val queryClazz = loadClass(className)
        val queryMethods = queryClazz.getDeclaredMethods

        var rtn: Method = null;
        queryMethods.foreach( (method) => {
            if ( method.getName.equals(queryName) ) {
                rtn = method
            }
        })
        rtn
    }

    def hasQueryMethod(className: String, queryName: String): Boolean = {
        getQueryMethod(className,queryName) != null
    }

    def setFieldValue(field: Field, fieldValue: String) = {
        if ( field.isInstanceOf[StringField] ) {
            field.asInstanceOf[StringField].apply(fieldValue)
        } else if ( field.isInstanceOf[IntegerField] ) {
            field.asInstanceOf[IntegerField].apply(fieldValue.toInt)
        } else if ( field.isInstanceOf[BooleanField] ) {
            field.asInstanceOf[BooleanField].apply(fieldValue.toBoolean)
        } else {
            throw new IllegalArgumentException("Unsupported field type found: " + field.getClass)
        }
    }

    def getFieldClass(typeName:String):Field = typeName match {
        case "string" => new StringField
        case "int"    => new IntegerField
        case "bool"   => new BooleanField
        case _        => throw new IllegalArgumentException("must be string, int, or bool")
    }

    def convertToClass(clazz: Class[_], value: String): Object = {
        if ( clazz.isPrimitive ) {
            if ( clazz.equals(java.lang.Integer.TYPE) ) {
                return new java.lang.Integer(value)
            } else if ( clazz.equals(java.lang.Boolean.TYPE) ) {
                return new java.lang.Boolean(value)
            } else {
                throw new IllegalArgumentException("Cannot convert to class: " + clazz.getName)
            }
        } else {
            val stringClass = Class.forName("java.lang.String")
            val intClass = Class.forName("java.lang.Integer")
            val boolClass = Class.forName("java.lang.Boolean")
            if ( clazz.equals(stringClass) ) {
                return value
            } else if ( clazz.equals(intClass) ) {
                return new java.lang.Integer(value)
            } else if ( clazz.equals(boolClass) ) {
                return new java.lang.Boolean(value)
            } else {
                throw new IllegalArgumentException("Cannot convert to class: " + clazz.getName)
            }
        }
    }

    def unravelException(e: Throwable): Throwable = {
        var prev = e
        var cause = e.getCause
        while ( cause != null ) {
            prev = cause
            cause = cause.getCause
        }
        return prev
    }

    def executeQuery(ent: Object, queryClass: String, queryName: String, queryInputs: Array[String]): Seq[Entity] = {

        val queryMethod = getQueryMethod(queryClass,queryName)
        if ( queryMethod == null ) {
            throw new IllegalArgumentException("invalid query name " + queryName + " in class " + queryClass)
        }

        val queryMethodParams: Array[Class[_]] = queryMethod.getParameterTypes
        if ( queryMethodParams.length != queryInputs.size + 1 ) {
            throw new IllegalArgumentException("invalid query input arg size given, expected " + (queryMethodParams.length-1) + " but got " + queryInputs.size)
        }

        val queryMethodInputs = new Array[Object](queryMethodParams.length)
        for ( i <- 0 to (queryInputs.size-1) ) {
            val obj = convertToClass(queryMethodParams(i), queryInputs(i))
            queryMethodInputs(i) = obj
        }
        queryMethodInputs(queryMethodParams.length-1) = env

        llogger.debug("query method params: ")
        queryMethodParams.foreach(llogger.debug(_))

        llogger.debug("query method inputs: " )
        queryMethodInputs.foreach(llogger.debug(_))

        try {
            val retVal: AnyRef = queryMethod.invoke(ent, queryMethodInputs : _*)
            if ( retVal == null ) {
                return null
            } else {
                return retVal.asInstanceOf[Seq[Entity]]
            }
        } catch {
            case e => {
                throw unravelException(e)
            }
        }
    }

    def fillCompositeField(field:CompositeField, compKey: Node):Unit = {
        if ( compKey.label.equals("compositekey") ) {
            llogger.debug("compositekey found:" + compKey)
            llogger.debug("compkey child: " + (compKey\"_"))
            (compKey\"_").toList.zip(field.fields).foreach( (tuple) => {
                val node = tuple._1
                val field = tuple._2
                llogger.debug("Looking at node: " + node)
                llogger.debug("Looking at field: " + field)
                if ( node.label.equals("attribute") ) {
                    setFieldValue(field, node.text)
                } else {
                    fillCompositeField(field.asInstanceOf[CompositeField], node)
                }
            })
        } else {
            throw new IllegalArgumentException("Must pass in composite key node")
        }
    }

    "a " + specName + " spec file" should {

        "produce a usable tookit by" in  {

            shareVariables()
            var _source = ""

            "parsing correctly" in {
                (_source = Compiler.codeGenFromSource(specSource))
								true must_== true
            }

            "compiling and packaging into jar correctly" in {
                makeNecessaryDirs()
                cleanup()

                val rb = ResourceBundle.getBundle("test-classpath")
                val classpath = rb.getString("maven.test.classpath")

                if ( classpath == null || classpath.isEmpty ) {
                    llogger.debug("Not running under mvn test jar bootstrap env")
                    Compiler.compileSpecCode(classfilesDir, _source) must not(throwA[Exception])
                } else {
                    llogger.debug("Running under mvn test jar bootstrap env")
                    Compiler.compileSpecCode(classfilesDir, classpath, _source) must not(throwA[Exception])
                }
								Compiler.tryMakeJar(jarFile, classfilesDir)
            }

            "loading entity classes correctly" in {

                classNameMap.keys.foreach( (c) => {

                    "for entity class " + c  in {
                        val entClazz = loadClass(c).asInstanceOf[Class[Entity]]
                        val entClazz2 = loadClass(c).asInstanceOf[Class[Entity]]
                        entClazz must notBeNull
                        entClazz mustEqual entClazz2
                        val entConstructor = entClazz.getConstructor(env.getClass)
                        entConstructor must not(throwA[NoSuchMethodException])
                        val ent = entConstructor.newInstance(env)
                        ent must not(throwA[Exception])
                        ent must haveSuperClass[Entity]
                        classNameMap(c) must haveTheSameElementsAs(ent.attributes.keySet)
                    }

                })

            }

            "generate the correct entity structure" in {

                "correct primary key field types" in {
                    pkMetadata.keys.foreach( (c) => {
                        "for entity class " + c  in {
                            val entClazz = loadClass(c).asInstanceOf[Class[Entity]]
                            val entConstructor = entClazz.getConstructor(env.getClass)
                            val ent = entConstructor.newInstance(env)
                            llogger.debug("PK in map: " + pkMetadata(c))
                            llogger.debug("Ent PK: " + ent.primaryKey)
                            val inputPK = CompositeField(pkMetadata(c).map(_._2):_*)
                            val entPK = ent.primaryKey
                            if (!inputPK.getClass.asInstanceOf[Class[Field]].isAssignableFrom(entPK.getClass.asInstanceOf[Class[Field]])) {
                                fail(inputPK.getClass + " is not assignable from " + entPK.getClass)
                            } else {
                                true must_== true
                            }
                            if ( inputPK.isInstanceOf[CompositeField] ) {
                                val inputPKasCF = inputPK.asInstanceOf[CompositeField]
                                val entPKasCF = entPK.asInstanceOf[CompositeField]
                                inputPKasCF.types.size must_== entPKasCF.types.size
                                entPKasCF.types.zip(inputPKasCF.types).foreach(
                                    (tuple) => { tuple._2.isAssignableFrom(tuple._1) must_== true })
                            }
                        }
                    })
                }

                "correct attribute field types" in {
                    attrMetadata.keys.foreach( (c) => {
                        "for entity class " + c  in {
                            val entClazz = loadClass(c).asInstanceOf[Class[Entity]]
                            val entConstructor = entClazz.getConstructor(env.getClass)
                            val ent = entConstructor.newInstance(env)
                            attrMetadata(c).foreach( (tuple) => {
                                "for attribute " + tuple._1 in {
                                    val inputAttr = tuple._2
                                    val entAttr = ent.attributes(tuple._1)
                                    if (!inputAttr.getClass.asInstanceOf[Class[Field]].isAssignableFrom(entAttr.getClass.asInstanceOf[Class[Field]])) {
                                        fail(inputAttr.getClass + " is not assignable from " + entAttr.getClass)
                                    } else {
                                        true must_== true
                                    }
                                    if ( inputAttr.isInstanceOf[CompositeField] ) {
                                        val inputAttrasCF = inputAttr.asInstanceOf[CompositeField]
                                        val entAttrasCF = entAttr.asInstanceOf[CompositeField]
                                        inputAttrasCF.types.size must_== entAttrasCF.types.size
                                        entAttrasCF.types.zip(inputAttrasCF.types).foreach(
                                            (tuple) => { tuple._2.isAssignableFrom(tuple._1) must_== true })
                                    }
                                }
                            })
                        }
                    })
                }


            }

            "creating the appropriate query methods" in {

                for ( (queryClass, queryArray) <- queries ) {
                    queryArray.foreach( (queryName) => {
                        "query method " + queryName + " in class " + queryClass in {
                            val queryMethod = getQueryMethod(queryClass,queryName)
                            if ( queryMethod == null ) {
                                fail("Could not find query: " + queryName + " in class: " + queryClass)
                            } else {
                                // trick to get custom error messages
                                queryMethod must notBeNull
                            }
                        }
                    })
                }

            }

            "persisting data appropriately" in {
                //val dataXMLFileObj = new File(dataXMLFile)
                //if ( !dataXMLFileObj.isFile ) {
                //    fail("No such input data XML file: " + dataXMLFile)
                //}

                //val dataNode = scala.xml.XML.loadFile(dataXMLFile)
                //if ( (dataNode \\ "entity").length == 0 ) {
                //    fail("No entity data given to test input")
                //}

                (dataNode \\ "entity").foreach( (entity) => {

                    val clazz = (entity \ "@class").text
                    llogger.debug("found class : " + clazz)

                    var ent:Entity = loadClass(clazz)
                        .asInstanceOf[Class[Entity]]
                        .getConstructor(env.getClass)
                        .newInstance(env)

                    val entityDesc = (entity \\ "attribute").map( x => (x\"@name").text + "->" + x.text ).toArray.deepMkString("[",",","]")

                    "for entity " + clazz + " (" + entityDesc + ")" in {
                        (entity \ "attributes" \ "attribute").foreach( (attribute) => {
                            val attributeName = (attribute \ "@name").text
                            llogger.debug("found attr name: " + attributeName)
                            val field: Field = ent.attributes(attributeName)
                            setFieldValue(field, attribute.text)
                        })
                        (entity \\ "foreign-key").foreach( (foreignKey) => {
                            val relationshipName = (foreignKey \ "@relationship").text
                            if ( (foreignKey\"@primarykey").size > 0 ) {
                                llogger.debug("found a primary key declaration")
                                val relationshipPK = (foreignKey \ "@primarykey").text
                                llogger.debug("found foreign key relationship: " + relationshipName)
                                val field: Field = ent.attributes(relationshipName)
                                setFieldValue(field, relationshipPK)
                            } else if ( (foreignKey\"compositekey").size > 0 ) {
                                llogger.debug("found a composite key declaration")
                                ent.attributes(relationshipName) must haveSuperClass[CompositeField]
                                val compositeField: CompositeField = ent.attributes(relationshipName).asInstanceOf[CompositeField]
                                val attributes = foreignKey \\ "attribute"
                                attributes.size must_== compositeField.types.size
                                attributes.toList.zip(compositeField.fields).foreach((tuple)=>{
                                    setFieldValue(tuple._2,tuple._1.text)
                                })

                            } else {
                                llogger.debug("found no foreign keys")
                            }
                        })
                        //(ent.save) must not(throwA[Exception])
                        try{
                            ent.save
                        } catch {
                            case e: Exception => { llogger.fatal(e); fail("could not serialize") }
                        }
                        true must_== true // hack...
                    }
                })
            }

            "passing the query tests" in {
                val queriesXMLFileObj = new File(queriesXMLFile)
                if ( !queriesXMLFileObj.isFile ) {
                    fail("No such input queries XML file: " + queriesXMLFile)
                }

                val queryNode = scala.xml.XML.loadFile(queriesXMLFile)
                if ( (queryNode \\ "query").length == 0 ) {
                    fail("No query tests given to test input")
                }

                (queryNode \\ "query").foreach( (query) => {

                    val queryName = (query \ "@name").text
                    val queryClass = (query \ "@class").text
                    val queryPK = (query \ "@primarykey").text

                    "for query with name " + queryName + " of class " + queryClass in {

                        var ent: Entity = null
                        if ( queryPK == null || queryPK.isEmpty ) {
                            // assume static query class
                            llogger.debug("Assuming query " + queryName + " is static method")
                        } else {
                            // need to create an instance of the entity class
                            llogger.debug("Need to create instance of "
                                + queryClass + " for query " + queryName)
                            // convention is to assume there exists a query
                            // called find[EntityClassName]ByPK in class
                            // Queries
                            var entSeq: Seq[Entity] = null
                            val pkQueryName = "find" + queryClass + "ByPK"
                            if ( !hasQueryMethod("Queries", pkQueryName) ) {
                                fail("No such query " + pkQueryName + " found in class Queries, but needed for testing query method " + queryName)
                            }
                            entSeq = executeQuery(null, "Queries", pkQueryName, Array[String](queryPK))
                            if ( entSeq.size != 1 ) {
                                fail("primary key lookup on class " + queryClass + " did not return one element, but instead returned " + entSeq.size)
                            }
                            ent = entSeq(0)
                            val queryClassObj = loadClass(queryClass)
                            ent.getClass.asInstanceOf[Class[Entity]] mustEqual queryClassObj
                        }
                        val queryInputs = query \\ "input"

                        llogger.debug("executing query: " + queryName + " with input(s): " + queryInputs)
                        val retVal = executeQuery(ent, queryClass, queryName, queryInputs.map(_.text).toArray)
                                                                                                               llogger.debug("query output: " + retVal)
                        retVal must notBeNull

                        val inOrder = (query \ "outputs" \ "@inorder").text.equals("true")
                        val outputClass = (query \ "outputs" \ "@class").text
                        val queryOutputs = query \\ "output"

                        retVal.size must_== queryOutputs.size

                        //val queryOutputPKs: Seq[String] = queryOutputs.map( (node) => {
                        //    (node \ "@primarykey").text
                        //})

                        //val actualOutputPKs: Seq[String] = retVal.map( (ent) => {
                        //    ent.primaryKey.asInstanceOf[ValueHoldingField[_]].value.toString
                        //})

                        val isCompositeKey = !(queryOutputs \ "compositekey").isEmpty

                        var queryOutputPKs: List[Field] = null
                        var actualOutputPKs: List[Field] = retVal.toList.map(_.primaryKey)

                        queryOutputPKs = queryOutputs.toList.zip(retVal.toList).map( (tuple) => {
                            val node = tuple._1
                            val ent = tuple._2
                            val field = ent.primaryKey.duplicate
                            if (isCompositeKey) {
                                val attrList = (node \\ "attribute" \ "@name").map(_.text)
                                field must haveClass[CompositeField]
                                fillCompositeField(field.asInstanceOf[CompositeField],(node\"compositekey").first)
                            } else {
                                val text = (node \ "@primarykey").text
                                setFieldValue(field, text)
                            }
                            field
                        })

                        if ( inOrder ) {
                            queryOutputPKs must containInOrder(actualOutputPKs)
                        } else {
                            queryOutputPKs must haveTheSameElementsAs(actualOutputPKs)
                        }

                    }

                })
            }

        }

    }



}
