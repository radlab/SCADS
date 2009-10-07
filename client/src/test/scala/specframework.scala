package edu.berkeley.cs.scads.test

import org.specs._
import org.specs.runner.JUnit4

import edu.berkeley.cs.scads.Compiler
import edu.berkeley.cs.scads.model.{Entity,BooleanField,IntegerField,StringField,Environment}

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
    val specFile: String
    val classNameMap: Map[String,Array[String]]
    val dataXMLFile: String
    val queries: Array[String]
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

    def getQueryMethod(name: String): Method = {
        val queryClazz = loadClass("Queries")
        val queryMethods = queryClazz.getDeclaredMethods
    
        var rtn: Method = null;
        queryMethods.foreach( (method) => {
            if ( method.getName.equals(name) ) {
                rtn = method 
            }
        })
        rtn
    }

    def getVariableTuple(varType: String, varValue: String ): (Class[Object],Object) = {
        varType match {
            case "string" => (Class.forName("java.lang.String").asInstanceOf[Class[Object]],varValue)
            case "int" => (Class.forName("java.lang.Integer").asInstanceOf[Class[Object]],new Integer(varValue))
            case "bool" => (Class.forName("java.lang.Boolean").asInstanceOf[Class[Object]],new java.lang.Boolean(varValue))
            case _ => throw new IllegalArgumentException
        }
    }

    "a scads spec file" should {

        "produce a usable tookit by" in  {

            shareVariables()
            var _source = ""

            "parsing correctly" in {
                try {
                    _source = Compiler.codeGenFromSource(specSource)  
                } catch {
                    case ex: Exception => fail("unable to parse")
                }
            }

            "compiling and packaging into jar correctly" in {
                makeNecessaryDirs()
                cleanup()

                val rb = ResourceBundle.getBundle("test-classpath")
                val classpath = rb.getString("maven.test.classpath")

                try {
                    Compiler.compileSpecCode(classfilesDir, jarFile, classpath, _source)
                } catch {
                    case ex: Exception => fail("unable to compile")
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
                    ent must notBeNull
                    classNameMap(c) must haveTheSameElementsAs(ent.attributes.keySet)
                })

            }

            "create the appropriate query methods" in {
                
                queries.foreach( (name) => {
                    val queryMethod = getQueryMethod(name)
                    queryMethod must notBeNull
                })

            }

            "input data appropriately" in {

                val dataNode = scala.xml.XML.loadFile(dataXMLFile)

                (dataNode \\ "entity").foreach( (entity) => {
                    val clazz = (entity \ "@class").text
                    llogger.debug("found class : " + clazz)
                    val ent = loadClass(clazz)
                        .asInstanceOf[Class[Entity]]
                        .getConstructor(env.getClass)
                        .newInstance(env)
                    (entity \\ "attribute").foreach( (attribute) => {
                        val attributeName = (attribute \ "@name").text
                        val attributeType = (attribute \ "@type").text
                        llogger.debug("found attr name: " + attributeName)
                        val field = ent.attributes(attributeName)
                        val varTuple = getVariableTuple(attributeType, attribute.text)
                        attributeType match {
                            case "string" => field.asInstanceOf[StringField](varTuple._2.asInstanceOf[String])
                            case "int"    => field.asInstanceOf[IntegerField](varTuple._2.asInstanceOf[Int])
                            case "bool"   => field.asInstanceOf[BooleanField](varTuple._2.asInstanceOf[Boolean])
                            case _ => throw new IllegalArgumentException("cannot handle ")
                        }

                    })
                    ent.save
                })

                val queryNode = scala.xml.XML.loadFile(queriesXMLFile)

                (queryNode \\ "query").foreach( (query) => {
                    val queryName = (query \ "@name").text
                    val queryInputs: Seq[Tuple2[Class[Object],Object]] = 
                        (query \\ "input").map[Tuple2[Class[Object],Object]]( (input) => {
                            val typeName = (input \ "@type").text
                            val typeValue = input.text
                            getVariableTuple(typeName,typeValue)
                        })

                    val queryMethod = getQueryMethod(queryName)
                    queryMethod must notBeNull

                    val queryMethodParams = queryMethod.getParameterTypes
                    queryMethodParams.startsWith(queryInputs.map(_._1)) mustEqual true

                    queryMethodParams.foreach(llogger.debug(_))
                    llogger.debug("-------------")

                    val args = queryInputs.map(_._2).concat( Array(env) ).toArray
                    args.foreach(llogger.debug(_))
                    //try {
                        val retVal = queryMethod.invoke(null, args : _*) 
                        retVal must notBeNull
                    //} catch {
                     //   case ex: InvocationTargetException => println(ex.getCause.printStackTrace)

                    //}

                    var pkTypeVar = ""
                    val pKeySeq = retVal.asInstanceOf[Seq[Entity]].map( (ent) => { 
                        if ( ent.primaryKey.isInstanceOf[StringField] ) {
                            pkTypeVar = "string"
                            ent.primaryKey.asInstanceOf[StringField].value
                        } else if ( ent.primaryKey.isInstanceOf[IntegerField] ) {
                            pkTypeVar = "int"
                            ent.primaryKey.asInstanceOf[IntegerField].value
                        } else if ( ent.primaryKey.isInstanceOf[BooleanField] ) {
                            pkTypeVar = "bool"
                            ent.primaryKey.asInstanceOf[BooleanField].value
                        } else {
                            throw new Exception("cannot handle this kind of field")
                        }
                    })
                    pKeySeq.foreach(llogger.debug(_))
                    llogger.debug("---------")

                    val inputpKeySeq = (query \\ "@primarykey").map( (pk) => { 
                        getVariableTuple(pkTypeVar,pk.text)._2
                    })
                    inputpKeySeq.foreach(llogger.debug(_))

                    pKeySeq must haveTheSameElementsAs(inputpKeySeq)

                })


            }

        }

    }

}


