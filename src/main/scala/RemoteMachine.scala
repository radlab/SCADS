package deploylib

import java.io.{File, BufferedReader, InputStreamReader}
import org.apache.log4j.Logger
import ch.ethz.ssh2.{Connection, Session, ChannelCondition}

case class ExecuteResponse(status: Integer, stdout: String, stderr: String)

abstract class RemoteMachine {
	val hostname: String
	val username: String
	val privateKey: File

	val logger = Logger.getLogger("deploylib.remoteMachine")
	var connection: Connection = null

	def useConnection[ReturnType](func: (Connection) => ReturnType): ReturnType = {
		if(connection == null) {
			connection = new Connection(hostname)
			connection.connect()
			connection.authenticateWithPublicKey(username, privateKey, "")
		}
		func(connection)
	}

	def executeCommand(cmd: String): ExecuteResponse = {
		useConnection((c) => {
			val stdout = new StringBuilder
    	val stderr = new StringBuilder

			val session = connection.openSession
			val outReader = new BufferedReader(new InputStreamReader(session.getStdout()))
			val errReader = new BufferedReader(new InputStreamReader(session.getStderr()))

			logger.debug("Executing: " + cmd)
			session.execCommand(cmd)

			var continue = true
			var exitStatus:Integer = null
			while(continue) {
				val status = session.waitForCondition(ChannelCondition.STDOUT_DATA |
																							ChannelCondition.STDERR_DATA |
																						 	ChannelCondition.EXIT_STATUS |
																						 	ChannelCondition.EXIT_SIGNAL |
																							ChannelCondition.EOF |
																							ChannelCondition.CLOSED, 0)
				if((status & ChannelCondition.STDOUT_DATA) != 0) {
					while(outReader.ready) {
						val line = outReader.readLine()
						if(line != null) {
							logger.debug("Received STDOUT_DATA: " + line)
							stdout.append(line)
						}
					}
				}
				if((status & ChannelCondition.STDERR_DATA) != 0) {
					while(errReader.ready) {
						val line = errReader.readLine()
						if(line != null) {
							logger.debug("Received STDERR_DATA: " + line)
							stderr.append(line)
						}
					}
				}
				if((status & ChannelCondition.EXIT_STATUS) != 0) {
					logger.debug("Received EXIT_STATUS")
					exitStatus = session.getExitStatus()
					continue = false
				}
				if((status & ChannelCondition.EXIT_SIGNAL) != 0) {
					logger.debug("Received EXIT_SIGNAL: " + session.getExitSignal())
					continue = false
				}
				if((status & ChannelCondition.EOF) != 0) {
					logger.debug("Received EOF")
				}
				if((status & ChannelCondition.CLOSED) != 0) {
					logger.debug("Received CLOSED")
				}
			}

			ExecuteResponse(exitStatus, stdout.toString, stderr.toString)
		})
	}

	override def toString(): String = "<RemoteMachine " + username + "@" + hostname + ">"
}
