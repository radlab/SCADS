package edu.berkeley.cs.scads.test

import org.specs._
import org.specs.runner.JUnit4

import edu.berkeley.cs.scads.Compiler
import edu.berkeley.cs.scads.model.{Entity,StringField,Environment}

import edu.berkeley.cs.scads.model.{TrivialExecutor,TrivialSession}
//import edu.berkeley.cs.scads.TestCluster

import java.io.File
import java.net.URLClassLoader
import java.util.ResourceBundle



abstract class ScadsLangSpec extends SpecificationWithJUnit("SCADS Lang Specification") {
    
    val specFile: String
    val classNameMap: Map[String,Array[String]]
    val dataXMLFile: String

    implicit val env = new Environment
    env.placement = new TestCluster
    env.session = new TrivialSession
    env.executor = new TrivialExecutor

    val specSource = getSourceFromFile(specFile)
    val baseDir = new File("src/main/scala/generated")
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
       // classLoader.loadClass(name).asInstanceOf[Class[Entity]].newInstance
        classLoader.loadClass(name).asInstanceOf[Class[Any]]
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

            "input data appropriately" in {

                val dataNode = scala.xml.XML.loadFile(dataXMLFile)

                (dataNode \\ "entity").foreach( (entity) => {
                    val clazz = (entity \ "@class").text
                    println("found class : " + clazz)
                    val ent = loadClass(clazz).asInstanceOf[Class[Entity]].getConstructor(env.getClass).newInstance(env)
                    (entity \\ "attribute").foreach( (attribute) => {
                        val attributeName = (attribute \ "@name").text
                        val attributeType = (attribute \ "@type").text
                        println("found attr name: " + attributeName)
                        val field = ent.attributes(attributeName)
                        attributeType match {
                            case "string" => field.asInstanceOf[StringField](attribute.text)
                            case _ => throw new Exception("invalid type " + attributeType)
                        }
                    })
                    ent.save
                })

            }

        }

    }

}


