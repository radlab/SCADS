package edu.berkeley.cs.scads.test

import org.specs._
import org.specs.runner.JUnit4

import edu.berkeley.cs.scads.Compiler
import edu.berkeley.cs.scads.model.Entity

import java.io.File

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


    }

}

object ScadrLangSpec extends ScadsLangSpec {
    val specFile = new File("scadr.scads") 
    val classNameMap = Map(
        "user" -> Array("username")
            )
}
