package deploylib

import java.io.File

abstract class RunitTemplate {
	val shell = "#!/bin/sh"
	val redirect = "exec 2>&1"
	
	val name: String
	val command: String

	protected def setup(target: RunitController): Unit = null
	protected def buildCommand(target: RunitController): String = Array(shell, redirect, "exec " + command).mkString("", "\n", "") 

	def deploy(target: RunitController) {
		val path = new File(target.serviceRoot, name)
		val runCmd = new File(path, "run")
		val logDir = new File(path, "log")
		val logCmd = new File(logDir, "run")	

		val logger = "#!/bin/sh\nexec " + target.svlogdCmd + " -tt ./"

		setup(target)
		target.executeCommand("mkdir -p " + logDir)
		target.createFile(runCmd, buildCommand(target))
		target.createFile(logCmd, logger)
		target.executeCommand("chmod 755 " + logCmd)
		target.executeCommand("chmod 755 " + runCmd)
	}
}

object EchoTest extends RunitTemplate {
	val name = "echotest"
	val command = "echo 'hello'"
}


abstract trait RunitController extends RemoteMachine {
	val runitBinaryPath: File
	lazy val runsvdirCmd: File = new File(runitBinaryPath, "runsvdir")
	lazy val svCmd: File = new File(runitBinaryPath, "sv")
	lazy val svlogdCmd: File = new File(runitBinaryPath, "svlogd")

	def services: Array[Service] = {
		executeCommand("find " + serviceRoot) match {
			case ExecuteResponse(Some(0), services, "") => services.split("\n").map(new Service(this, _))
			case e: ExecuteResponse => {
				logger.fatal("Unexpected ExecuteResponse while listing services: " + e)
				Array[Service]()
			}
		}	
	}
}
