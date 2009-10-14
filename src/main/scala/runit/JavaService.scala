package deploylib.runit

import java.io.File

/**
 * Template for deploying Runit Services that are written in Java
 */
abstract class JavaService(localJar: File, className: String, args: String) extends RunitTemplate {
	val name = className
	val command = "no command"

	override def setup(target: RunitManager):Unit = {
		target.upload(localJar, target.rootDirectory)
	}

	override def buildCommand(target: RunitManager): String = {
		shell + "\n" +
		redirect + "\n" + 
		"/usr/lib/jvm/java-6-sun/bin/java -cp " + new File(target.rootDirectory, localJar.getName) + " " + className + " " + args
	}
}
