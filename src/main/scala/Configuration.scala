package deploylib.configuration

import scala.collection.mutable.ListBuffer
import deploylib.configuration.ValueConverstion._
import java.io.File

/* Support to late binding of Values */
abstract trait Value[Type] {
	def getValue(rm: RemoteMachine): Type
	override def toString: String
}

class LateBoundValue[Type](description: String, func: RemoteMachine => Type) extends Value[Type] {
	def getValue(rm: RemoteMachine): Type = func(rm)
	override def toString: String = "<LB " + description + ">"
}

class LateStringBuilder(parts: Value[String]*) extends Value[String] {
	def getValue(rm: RemoteMachine): String = parts.map(_.getValue(rm)).mkString("", "", "")
	override def toString: String = parts.map(_.toString).mkString("", "", "")
}

class ConstantValue[Type](value: Type) extends Value[Type] {
	def getValue(rm: RemoteMachine): Type = value
	override def toString: String = value.toString
}

object ValueConverstion {
	implicit def toConstantValue[Type](value: Type): ConstantValue[Type] = new ConstantValue[Type](value)
}

/* Structure of a configuration Node */
abstract class Configuration {
	def action(target: RemoteMachine)
	def description: String
}

abstract class CompositeConfiguration extends Configuration {
	val children = new ListBuffer[Configuration]

	def action(target: RemoteMachine) = {
		children.foreach({(c) =>
			//logger.debug("Executing: " + c)
			c.action(target)
		})
	}
}

class RemoteDirectory(parent: RemoteDirectory, name: Value[String]) extends Configuration with Value[String] {
	def this(name: Value[String]) = this(null, name)

	def action(target: RemoteMachine) = target.executeCommand("mkdir -p " + getValue(target))
	def description:String = "Create Directory " + toString

	override def toString: String = {
		if(parent == null)
			name.toString
		else
			parent + "/" + name.toString
	}

	def getValue(target: RemoteMachine):String = {
		if(parent == null)
			name.getValue(target)
		else
			parent.getValue(target) + "/" + name.getValue(target)
	}
}

class RemoteFile(dest: RemoteDirectory, filename: String, contents: Value[String], mode: String) extends Configuration with Value[String]{
	def action(target: RemoteMachine) = {
		val filePath = dest.getValue(target) + "/" + filename
		target.createFile(new File(filePath), contents.getValue(target))
		target.executeCommand("chmod " + mode + " " + filePath)
	}
	def description: String = "Create file " + filename + " at " + dest + " with contents " + contents

	def getValue(target: RemoteMachine) = dest.getValue(target) + "/" + filename
}

class FileUpload(localFile: File, dest: RemoteDirectory) extends Configuration with Value[String] {
	def action(target: RemoteMachine) = {
		target.upload(localFile, new File(dest.getValue(target)))
	}
	def description: String = "Upload " + localFile + " to " + dest.toString

	def getValue(target: RemoteMachine) = dest.getValue(target) + "/" + localFile.getName()
}

object MachineRoot extends RemoteDirectory(null, new LateBoundValue("MachineRoot", (t: RemoteMachine) => t.rootDirectory.toString))
object ServiceRoot extends RemoteDirectory(MachineRoot, "services")

object RunitService {
	val shell = "#!/bin/sh\n"
	val redirect = "exec 2>&1\n"
	val header = shell + redirect + "exec "
}

class RunitService(name: String, runCommand: Value[String], logCommand: Value[String]) extends CompositeConfiguration {
	val baseDirectory =  new RemoteDirectory(ServiceRoot, name)
	val logDirectory = new RemoteDirectory(baseDirectory, "log")
	val runFile = new RemoteFile(baseDirectory, "run", runCommand, "755")
	val logFile = new RemoteFile(logDirectory, "run", logCommand, "755")
	children ++= List(baseDirectory, logDirectory, runFile, logFile)

	def description: String = "Create RunitService " + name + " w/ cmd " + runCommand + " and logCmd " + logCommand
}

class DefaultLoggedRunitService(name: String, runCommand: Value[String]) extends
	RunitService(name, runCommand, new LateBoundValue("logger to <bindir>/svlogdCmd", (target: RemoteMachine) => "#!/bin/sh\nexec " + target.svlogdCmd + " -tt ./"))

class JavaService(localJar: File, className: String, args: String) extends CompositeConfiguration {
	val remoteJar = new FileUpload(localJar, MachineRoot)
	val runCmd = new LateStringBuilder(RunitService.header, "/usr/lib/jvm/java-6-sun/bin/java -cp .:", new LateBoundValue("JarLocation" ,remoteJar.getValue(_)), " ", className, " ", args)
	val service = new DefaultLoggedRunitService(className, runCmd)
	val log4jProperties = new RemoteFile(service.baseDirectory,
																			 "log4j.properties",
																			 Array("log4j.rootLogger=DEBUG, stdout",
																						 "log4j.appender.stdout=org.apache.log4j.ConsoleAppender",
																						 "log4j.appender.stdout.layout=org.apache.log4j.PatternLayout",
																						 "log4j.appender.stdout.layout.ConversionPattern=[%-5p] %m%n").mkString("", "\n", "\n"), "644")


	children ++= List(remoteJar, service, log4jProperties)

	def description: String = "Create java service running " + className + " from " + localJar
}

object ScadsEngine extends JavaService(new File("../scads/scalaengine/target/scalaengine-1.0-SNAPSHOT-jar-with-dependencies.jar"), "edu.berkeley.cs.scads.storage.JavaEngine", "")
