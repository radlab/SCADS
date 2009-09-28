package edu.berkeley.cs.scads.test

import org.specs._
import org.specs.runner.JUnit4

import edu.berkeley.cs.scads.Compiler
import edu.berkeley.cs.scads.model.{Entity,StringField}

import java.io.File
import java.net.URLClassLoader
import java.util.ResourceBundle



abstract class ScadsLangSpec extends SpecificationWithJUnit("SCADS Lang Specification") {
    
    val specFile: String
    val classNameMap: Map[String,Array[String]]
    val dataXMLFile: String


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

    def loadEntityClass(name: String): Entity = {
        val classLoader = new URLClassLoader(Array(jarFile.toURI.toURL))
        classLoader.loadClass(name).asInstanceOf[Class[Entity]].newInstance
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
                    val ent = loadEntityClass(c)
                    classNameMap(c) must haveTheSameElementsAs(ent.attributes.keySet)
                })
            }

            "input data appropriately" in {

                val dataNode = scala.xml.XML.loadFile(dataXMLFile)

                (dataNode \\ "entity").foreach( (entity) => {
                    val clazz = (entity \ "@class").text
                    println("found class : " + clazz)
                    val ent = loadEntityClass(clazz)
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
                    //ent.save
                })

            }

        }

    }

}


