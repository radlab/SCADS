package edu.berkeley.cs.scads.test

import org.specs._
import org.specs.runner.JUnit4

import edu.berkeley.cs.scads.Compiler
import edu.berkeley.cs.scads.model.{Entity,BooleanField,IntegerField,Field,StringField,ValueHoldingField,Environment}

import edu.berkeley.cs.scads.model.{TrivialExecutor,TrivialSession}
//import edu.berkeley.cs.scads.TestCluster

import java.io.File
import java.net.URLClassLoader
import java.util.ResourceBundle

import java.lang.reflect.Method
import java.lang.reflect.InvocationTargetException

import org.apache.log4j.Logger

abstract class ScadsLangSpec extends SpecificationWithJUnit("SCADS Lang Specification") {
    val llogger = Logger.getLogger("scads.test")
    val specName: String
    val specFile: String
    val classNameMap: Map[String,Array[String]]
    val dataXMLFile: String
    val queries: Map[String,Array[String]]
    val queriesXMLFile: String

    implicit val env = new Environment
    env.placement = new TestCluster
    env.session = new TrivialSession
    env.executor = new TrivialExecutor

    val specSource = getSourceFromFile(specFile)
    val baseDir = new File("target/generated")
    val classfilesDir = new File(baseDir, "classfiles")
    val jarFile = new File(baseDir, "spec.jar")

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
        val classLoader = new URLClassLoader(Array(jarFile.toURI.toURL))
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

    def convertToClass(clazz: Class[_], value: String): Object = {
        val stringClass = Class.forName("java.lang.String")
        val intClass = Class.forName("java.lang.Integer")
        val boolClass = Class.forName("java.lang.Boolean")
        if ( clazz.equals(stringClass) ) {
            return value
        } else if ( clazz.equals(intClass) ) {
            return new Integer(value)
        } else if ( clazz.equals(boolClass) ) {
            return new java.lang.Boolean(value)
        } else {
            throw new IllegalArgumentException("Cannot convert to class: " + clazz)
        }
    }

    "a " + specName + " spec file" should {

        "produce a usable tookit by" in  {

            shareVariables()
            var _source = ""

            "parsing correctly" in {
                (_source = Compiler.codeGenFromSource(specSource)) must not(throwA[Exception])
            }

            "compiling and packaging into jar correctly" in {
                makeNecessaryDirs()
                cleanup()

                val rb = ResourceBundle.getBundle("test-classpath")
                val classpath = rb.getString("maven.test.classpath")

                if ( classpath == null || classpath.isEmpty ) {
                    Compiler.compileSpecCode(classfilesDir, jarFile, _source) must not(throwA[Exception])
                } else {
                    Compiler.compileSpecCode(classfilesDir, jarFile, classpath, _source) must not(throwA[Exception])
                }
            }

            "loading entity classes correctly" in {

                classNameMap.keys.foreach( (c) => {
                    val entClazz = loadClass(c).asInstanceOf[Class[Entity]]
                    entClazz must notBeNull
                    val entConstructor = entClazz.getConstructor(env.getClass)
                    entConstructor must not(throwA[NoSuchMethodException])
                    val ent = entConstructor.newInstance(env)
                    ent must not(throwA[Exception])
                    ent must haveSuperClass[Entity]
                    classNameMap(c) must haveTheSameElementsAs(ent.attributes.keySet)
                })

            }

            "create the appropriate query methods" in {

                for ( (queryClass, queryArray) <- queries ) {
                    queryArray.foreach( (queryName) => {
                        val queryMethod = getQueryMethod(queryClass,queryName)
                        if ( queryMethod == null ) {
                            fail("Could not find query: " + queryName + " in class: " + queryClass)
                        } else {
                            // trick to get custom error messages
                            queryMethod must notBeNull
                        }
                    })
                }

            }

            "input data appropriately" in {
                val dataXMLFileObj = new File(dataXMLFile)
                if ( !dataXMLFileObj.isFile ) {
                    fail("No such input data XML file: " + dataXMLFile)
                }

                val dataNode = scala.xml.XML.loadFile(dataXMLFile)
                if ( (dataNode \\ "entity").length == 0 ) {
                    fail("No entity data given to test input")
                }

                (dataNode \\ "entity").foreach( (entity) => {
                    val clazz = (entity \ "@class").text
                    llogger.debug("found class : " + clazz)
                    val ent = loadClass(clazz)
                        .asInstanceOf[Class[Entity]]
                        .getConstructor(env.getClass)
                        .newInstance(env)
                    (entity \\ "attribute").foreach( (attribute) => {
                        val attributeName = (attribute \ "@name").text
                        llogger.debug("found attr name: " + attributeName)
                        val field: Field = ent.attributes(attributeName)
                        setFieldValue(field, attribute.text)
                    })
                    (entity \\ "foreign-key").foreach( (foreignKey) => {
                        val relationshipName = (foreignKey \ "@relationship").text
                        val relationshipPK = (foreignKey \ "@primarykey").text
                        llogger.debug("found foreign key relationship: " + relationshipName)
                        val field: Field = ent.attributes(relationshipName)
                        setFieldValue(field, relationshipPK)
                    })
                    (ent.save) must not(throwA[Exception])
                })
            }

            "pass the query tests" in {
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

                    var ent: Entity = null
                    if ( queryPK == null || queryPK.isEmpty ) {
                        // assume static query class
                        llogger.debug("Assuming query " + queryName + " is static method")
                    } else {
                        // need to create an instance of the entity class
                        llogger.debug("Need to create instance of " 
                            + queryClass + " for query " + queryName)
                        fail("Instance method query testing is un-implemented for now") 
                    }
                    var queryInputs = query \\ "input"
                    val queryMethod = getQueryMethod(queryClass,queryName)
                    queryMethod must notBeNull

                    val queryMethodParams: Array[Class[_]] = queryMethod.getParameterTypes
                    queryMethodParams.length must be(queryInputs.size + 1)
                    val queryMethodInputs = new Array[Object](queryMethodParams.length)
                    for ( i <- 0 to (queryInputs.size-1) ) {
                        val obj = convertToClass(queryMethodParams(i), queryInputs(i).text)
                        queryMethodInputs(i) = obj
                    }
                    queryMethodInputs(queryMethodParams.length-1) = env

                    llogger.debug("query method params: ")
                    queryMethodParams.foreach(llogger.debug(_))

                    llogger.debug("query method inputs: " )
                    queryMethodInputs.foreach(llogger.debug(_))

                    val retVal = queryMethod.invoke(ent, queryMethodInputs : _*)
                    retVal must notBeNull

                    val inOrder = (query \ "outputs" \ "@inorder").text.equals("true")
                    val outputClass = (query \ "outputs" \ "@class").text
                    val queryOutputs = query \\ "output"
                    
                    val queryOutputPKs: Seq[String] = queryOutputs.map( (node) => {
                        (node \ "@primarykey").text
                    })

                    val actualOutputPKs: Seq[String] = retVal.asInstanceOf[Seq[Entity]].map( (ent) => {
                        ent.primaryKey.asInstanceOf[ValueHoldingField[_]].value.toString
                    })

                    if ( inOrder ) {
                        queryOutputPKs must containInOrder(actualOutputPKs)
                    } else {
                        queryOutputPKs must haveTheSameElementsAs(actualOutputPKs)
                    }

                })
            }

        }

    }

}
