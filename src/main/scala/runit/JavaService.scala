package deploylib.runit

import java.io.File

/**
 * Template for deploying Runit Services that are written in Java
 */
abstract class JavaService(localJar: File, className: String, args: String) extends RunitTemplate {
	val name = className
	val command = "no command"
	val logPropertiesContents = Array("log4j.rootLogger=DEBUG, stdout",
														"log4j.appender.stdout=org.apache.log4j.ConsoleAppender",
														"log4j.appender.stdout.layout=org.apache.log4j.PatternLayout",
														"log4j.appender.stdout.layout.ConversionPattern=[%-5p] %m%n").mkString("", "\n", "\n")

	override def setup(target: RunitManager):Unit = {
		val path = new File(target.serviceRoot, name)
		val logPropertiesFile = new File(path, "log4j.properties")

		target.createFile(logPropertiesFile, logPropertiesContents)
		target.upload(localJar, target.rootDirectory)
	}

	override def buildCommand(target: RunitManager): String = {
		val path = new File(target.serviceRoot, name)

		shell + "\n" +
		redirect + "\n" +
		"/usr/lib/jvm/java-6-sun/bin/java -cp " + path + ":" + new File(target.rootDirectory, localJar.getName) + " " + className + " " + args
	}
}
