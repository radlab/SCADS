package deploylib.configuration

import deploylib.configuration.ValueConverstion._

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


