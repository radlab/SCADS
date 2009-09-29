package edu.berkeley.cs.scads

import org.apache.log4j.Logger
import org.apache.log4j.BasicConfigurator

import java.io.{File, FileInputStream, FileOutputStream, FileWriter}
import java.util.jar.{JarEntry, JarOutputStream}
import scala.tools.nsc.{Global, Settings}
import scala.tools.nsc.reporters.ConsoleReporter
import scala.tools.nsc.util.BatchSourceFile

import edu.berkeley.cs.scads.model.parser._
import edu.berkeley.cs.scads.model.Entity

case class CompileException(error: String) extends Exception
case class SpecParseException(error: String) extends Exception

object Compiler extends ScadsLanguage {
	val logger = Logger.getLogger("scads.compiler")
	BasicConfigurator.configure()

    def codeGenFromSource(src: String): String = parse(src) match {
        case Success(result, _) => {
            logger.debug("AST: " + Printer(result))
            val boundSpec = Binder.bind(result)
            logger.debug("Bound Spec: " + boundSpec)

	    boundSpec.orphanQueries.foreach((q) => Optimizer.optimize(q._2))
	    boundSpec.entities.foreach((e) => e._2.queries.foreach((q) => Optimizer.optimize(q._2)))

            val source = ScalaGen(boundSpec)
            logger.debug(source)
                
            source
        }
        case f: NoSuccess => {
            throw new SpecParseException("could not parse spec")
        }
    }

	def main(args: Array[String]): Unit = {
		logger.info("Loading spec.")
		val src = scala.io.Source.fromFile(args(0)).getLines.foldLeft(new StringBuilder)((x: StringBuilder, y: String) => x.append(y)).toString

        //val outFile = new File(outputBaseFile, "spec.scala")
        //outFile.createNewFile
        //val outFileWriter = new FileWriter(outFile)

		logger.info("Parsing spec.")
        try {

            val code = codeGenFromSource(src)

            val outputBaseFile = new File("src/main/scala/generated")
            outputBaseFile.mkdirs
            val genDir = new File(outputBaseFile, "classfiles")
            genDir.mkdirs
            val jarFile = new File(outputBaseFile, "spec.jar")
            //outFileWriter.write(source)
            compileSpecCode(genDir, jarFile, code)

            //val entity : Entity = loadCompiledClass(jarFile, "user") 
            //entity.attributes.keys.foreach(println(_))

        }
        catch {
            case UnknownRelationshipException(rn) => logger.fatal("Unknown relationship referenced: " + rn)
            case UnknownAttributeException(qn, an) => logger.fatal("Unknown attribute '" + an + "' in query " + qn)
            case UnknownEntityException(en) => logger.fatal("Unknown entity referenced: " + en)
            case BadParameterOrdinals(qn) => logger.fatal("Bad parameter ordinals detected in query " + qn)
            case UnsupportedPredicateException(qn, p) => logger.fatal("Query " + qn + " contains the following unsupported predicate " + p)
            case SpecParseException(err) => logger.fatal("Scala parser errored: " + err)
            case CompileException(err) => logger.fatal("Scala compiler errored: " + err)
        }

	}


    def compileSpecCode(genDir: File, jarFile: File, classpath: String, contents: String):Boolean = {
        println("Compiling spec code")
        val settings = new Settings(error)

        settings.deprecation.value = true
        settings.unchecked.value = true
        settings.outdir.value = genDir.toString
        settings.classpath.value = classpath


        val reporter = new ConsoleReporter(settings)

        val compiler = new Global(settings, reporter)

        val batchFile = new BatchSourceFile("<scads spec>",contents.toCharArray)

        (new compiler.Run).compileSources(List(batchFile))

        if ( reporter.hasErrors ) {
            throw new CompileException("error occurred with severity" + reporter.severity)
        } else {
            println("Compliation succeeded, packaging into jar file")
            tryMakeJar(jarFile, genDir)
            return true
        }
    }


    def compileSpecCode(genDir: File, jarFile: File, contents: String):Boolean = {
        val settings = new Settings(error)
        compileSpecCode(genDir, jarFile, settings.classpath.value, contents)
    }

    /*
     * This doesn't really do anything for now, but we need it
     */
    def error(message: String) = {
        println("Error received " + message)
    }


    /*
     * Taken from Scala's ScriptRunner
     * http://scala-tools.org/scaladocs/scala-compiler/2.7.1/tools/nsc/ScriptRunner.scala.html
     */
    private def tryMakeJar(jarFile: File, sourcePath: File) = {
        try {
            val jarFileStream = new FileOutputStream(jarFile)
            val jar = new JarOutputStream(jarFileStream)
            val buf = new Array[Byte](10240)

            def addFromDir(dir: File, prefix: String) {
                for (entry <- dir.listFiles) {
                    if (entry.isFile) {
                        jar.putNextEntry(new JarEntry(prefix + entry.getName))

                        val input = new FileInputStream(entry)
                        var n = input.read(buf, 0, buf.length)
                        while (n >= 0) {
                            jar.write (buf, 0, n)
                            n = input.read(buf, 0, buf.length)
                        }
                        jar.closeEntry
                        input.close
                    } else {
                        addFromDir(entry, prefix + entry.getName + "/")
                    }
                }
            }

            addFromDir(sourcePath, "")
            jar.close
        } catch {
            case _:Error => jarFile.delete // XXX what errors to catch?
        }
    }


}
