package edu.berkeley.cs.scads.test

import org.specs._
import org.specs.runner.JUnit4

import edu.berkeley.cs.scads.Compiler
import edu.berkeley.cs.scads.model.Entity

import java.io.File
import java.net.URLClassLoader
import java.util.ResourceBundle

abstract class ScadsLangSpec extends SpecificationWithJUnit("SCADS Lang Specification") {
    
    val specFile: File
    val classNameMap: Map[String,Array[String]]


    val specSource = getSourceFromFile(specFile)
    val baseDir = new File("src/main/scala/generated")
    val classfilesDir = new File(baseDir, "classfiles") 
    val jarFile = new File(baseDir, jarFile)

    def getSourceFromFile(file: File): String = {
		scala.io.Source.fromFile(args(0)).getLines.foldLeft(new StringBuilder)((x: StringBuilder, y: String) => x.append(y)).toString
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
        classLoader.findClass(name).asInstanceOf[Class[Entity]].newInstance
    }

    "a scads spec file" should {
        shareVariables()
        var _source = ""

        "parse correctly" in {
            try {
                _source = Compiler.codeGenFromSource(specSource)  
            } catch {
                case ex: Exception => fail("unable to parse")
            }
        }

        "compile correctly" in {
            makeNecessaryDirs()
            cleanup()

            val rb = ResourceBundle.getBundle("test-classpath")
            val classpath = rb.getString("maven.test.classpath")

            try {
                compileSpecCode(classfilesDir, jarFile, classpath, _source)
            } catch {
                case ex: Exception => fail("unable to compile")
            }
        }

        "load entity classes correctly" in {
            
            classNameMap.keys.foreach( (c) => {
                val ent = loadEntityClass(c)
                classNameMap(c) must haveTheSameElementsAs(ent.attributes.keySet)
            })

        }

        


    }

}

object ScadrLangSpec extends ScadsLangSpec {
    val specFile = new File("scadr.scads") 
    val classNameMap = Map(
        "user" -> Array("username")
            )
}
