package deploylib.runit

import java.io.File

/**
 * A template for runit service creation
 */
abstract class RunitTemplate {
	val shell = "#!/bin/sh"
	val redirect = "exec 2>&1"
	
	val name: String
	val command: String

	protected def setup(target: RunitManager): Unit = null
	protected def buildCommand(target: RunitManager): String = Array(shell, redirect, "exec " + command).mkString("", "\n", "") 

	def deploy(target: RunitManager) {
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




