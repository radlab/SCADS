package deploylib

import java.io.File

abstract class JavaService(localJar: File, className: String, args: String) extends RunitTemplate {
	val name = className
	val command = "no command"

	override def setup(target: RunitController):Unit = {
		target.upload(localJar, target.rootDirectory)
	}

	override def buildCommand(target: RunitController): String = {
		shell + "\n" +
		redirect + "\n" + 
		"/usr/lib/jvm/java-6-sun/bin/java -cp " + new File(target.rootDirectory, localJar.getName) + " " + className + " " + args
	}
}

object ScadsEngine extends JavaService(new File("/Users/marmbrus/Workspace/scads/scalaengine/target/scalaengine-1.0-SNAPSHOT-jar-with-dependencies.jar"), "edu.berkeley.cs.scads.storage.JavaEngine", "")
