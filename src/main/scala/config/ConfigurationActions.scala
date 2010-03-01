package deploylib.config

import java.io.File

import deploylib.runit._
import deploylib.xresults._

import org.apache.log4j.Logger

object Config extends ConfigurationActions

trait ConfigurationActions {
	val logger = Logger.getLogger("deploylib.config")

	def createDirectory(target: RemoteMachine, directory: File): File = {
		target.executeCommand("mkdir -p " + directory)
		return directory
	}

	def createFile(target: RemoteMachine, filename: File, contents: String, mode: String): File = {
		target.createFile(filename, contents)
		target.executeCommand("chmod " + mode + " " + filename)
		return filename
	}

	def uploadFile(target: RemoteMachine, localFile: File, destination: File): File = {
		target.upload(localFile, destination)
		return new File(destination, localFile.getName)
	}

	def createRunitService(target: RunitManager, name: String, runCommand: String): RunitService =  createRunitService(target, name, runCommand, "#!/bin/sh\nexec " + target.svlogdCmd + " -tt ./")
	def createRunitService(target: RunitManager, name: String, runCommand: String, logCommand: String): RunitService = {
		val baseDirectory = createDirectory(target, new File(target.serviceRoot, name))
		val logDirectory = createDirectory(target, new File(baseDirectory, "log"))
		val downFile = createFile(target, new File(baseDirectory, "down"), " ", "644")
		val runFile = createFile(target, new File(baseDirectory, "run"), "#!/bin/sh\nexec 2>&1\nexec " + runCommand, "755")
		val logFile = createFile(target, new File(logDirectory, "run"), logCommand, "755")
		val finishFile = createFile(target, new File(baseDirectory, "finish"), "#!/bin/sh\necho FAILURE `/bin/date`: " + name + " $@ >> failures", "755")

		logger.debug("Waiting for runsvdir to notice " + name)
		target.blockTillFileCreated(new File(baseDirectory, "supervise/stat"))

		new RunitService(target, name)
	}

	def createJavaService(target: RunitManager, localJar: File, className: String, maxHeapMb: Int, args: String): RunitService = {
		val remoteJar = uploadFile(target, localJar, target.rootDirectory)
    val expIdFlag = if(XResult.experimentId != null) "-DexperimentId=" + XResult.experimentId else ""
    val jvmArgs = "-server -Xmx" + maxHeapMb + "m " + expIdFlag + " -XX:+HeapDumpOnOutOfMemoryError "
		val runCmd = target.javaCmd + " " +
                  jvmArgs + " " +
								 "-cp .:" + remoteJar + " " +
								 className + " " + args
		val service = createRunitService(target, className, runCmd)
		val log4jProperties = createFile(target, new File(service.serviceDir, "log4j.properties"),  Array("log4j.rootLogger=INFO, stdout",
																						 "log4j.appender.stdout=org.apache.log4j.ConsoleAppender",
																						 "log4j.appender.stdout.layout=org.apache.log4j.SimpleLayout").mkString("", "\n", "\n"), "644")

    XResult.storeXml(<configuration type="javaservice">
                      <target>{target.hostname}</target>
                      <localJar>{localJar}</localJar>
                      <className>{className}</className>
                      <maxHeap>{maxHeapMb.toString}</maxHeap>
                      <args>{args}</args>
											<cmdLine>{runCmd}</cmdLine>
                     </configuration>)

    return service
	}
}
