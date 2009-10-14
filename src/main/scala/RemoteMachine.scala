package deploylib

import java.io.{File, BufferedReader, InputStreamReader}
import org.apache.log4j.Logger
import ch.ethz.ssh2.{Connection, Session, ChannelCondition, SCPClient}

case class ExecuteResponse(status: Option[Int], stdout: String, stderr: String)

abstract class RemoteMachine {
	val hostname: String
	val username: String
	val privateKey: File
	val rootDirectory: File

	val logger = Logger.getLogger("deploylib.remoteMachine")
	lazy val serviceRoot = new File(rootDirectory, "services")
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

			ExecuteResponse(Some(exitStatus.intValue), stdout.toString, stderr.toString)
		})
	}

	def createFile(file: File, contents: String): Unit = {
			useConnection((c) => {
				val session = connection.openSession
				session.execCommand("cat > " + file)
				session.getStdin().write(contents.getBytes)
				session.getStdin().close()
				session.close()
			})
	}

	def upload(localFile: File, remoteDirectory: File):Unit = upload(localFile.toString, remoteDirectory.toString)
	def upload(localFile: String, remoteDirectory: String): Unit = {
		useConnection((c) => {
			val scp = new SCPClient(connection)
			scp.put(localFile, remoteDirectory)
		})
	}

	def download(remoteFile: String, localDirectory: String): Unit = {
		val scp = new SCPClient(connection)
		scp.get(remoteFile, localDirectory)
	}

	override def toString(): String = "<RemoteMachine " + username + "@" + hostname + ">"
}
