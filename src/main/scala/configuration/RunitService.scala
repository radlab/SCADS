package deploylib.configuration

import deploylib.configuration.ValueConverstion._

@deprecated()
object ServiceRoot extends RemoteDirectory(MachineRoot, "services")

@deprecated()
object RunitService {
	val shell = "#!/bin/sh\n"
	val redirect = "exec 2>&1\n"
	val header = shell + redirect + "exec "
}

@deprecated()
class RunitService(name: String, runCommand: Value[String], logCommand: Value[String]) extends CompositeConfiguration {
	val baseDirectory =  new RemoteDirectory(ServiceRoot, name)
	val logDirectory = new RemoteDirectory(baseDirectory, "log")
  val downFile = new RemoteFile(baseDirectory, "down", " ", "644")
	val runFile = new RemoteFile(baseDirectory, "run", runCommand, "755")
	val logFile = new RemoteFile(logDirectory, "run", logCommand, "755")
	children ++= List(baseDirectory, logDirectory, downFile, runFile, logFile)

	def description: String = "Create RunitService " + name + " w/ cmd " + runCommand + " and logCmd " + logCommand
}

@deprecated()
class DefaultLoggedRunitService(name: String, runCommand: Value[String]) extends
	RunitService(name, runCommand, new LateBoundValue("logger to <bindir>/svlogdCmd", (target: RemoteMachine) => "#!/bin/sh\nexec " + target.svlogdCmd + " -tt ./"))
