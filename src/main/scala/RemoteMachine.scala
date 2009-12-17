package deploylib

import java.io.{File, BufferedReader, InputStreamReader}
import org.apache.log4j.Logger
import ch.ethz.ssh2.{Connection, Session, ChannelCondition, SCPClient}

/**
 * Holds the results of the execution of a command on the remote machine.
 * @param status - the integer status code returned by the command if one was returned, otherwise None
 * @param stdout - a string with all of the data that was output on STDOUT by the command
 * @param stderr - a string with all of the data that was output on STDERR by the command
 */
case class ExecuteResponse(status: Option[Int], stdout: String, stderr: String)
case class RemoteFile(name: String, owner: String, permissions: String, modDate: String, size: String)

case class UnknownResponse(er: ExecuteResponse) extends Exception

/**
 * Provides a framework for interacting (running commands, uploading/downloading files, etc) with a generic remote machine.
 */
abstract class RemoteMachine {
	/**
	 * The hostname that the ssh connection is established with
	 */
	val hostname: String

	/**
	 * The username used to authenticate with the remote ssh server
	 */
	val username: String

	/**
	 * The private key used to authenticate with the remote ssh server
	 */
	val privateKey: File

	/**
	 * The default root directory for operations (The user should have read write permissions to this directory)
	 */
	val rootDirectory: File

	val javaCmd: File

	val runitBinaryPath: File
	lazy val runsvdirCmd: File = new File(runitBinaryPath, "runsvdir")
	lazy val svCmd: File = new File(runitBinaryPath, "sv")
	lazy val svlogdCmd: File = new File(runitBinaryPath, "svlogd")
	lazy val serviceRoot = new File(rootDirectory, "services")

	val logger = Logger.getLogger("deploylib.remoteMachine")
	private var connection: Connection = null

	/**
	 * Provide an ssh connection to the server.  If one is not already available or has been disconnected, create one.
	 */
	protected def useConnection[ReturnType](func: (Connection) => ReturnType): ReturnType = {
		if(connection == null) {
			connection = new Connection(hostname)
            logger.info("Connecting to " + hostname)
			connection.connect()
            logger.info("Authenticating with username " + username + " privateKey " + privateKey)
			connection.authenticateWithPublicKey(username, privateKey, "")
		}
		try {
		func(connection)
		}
		catch {
      case ioe: java.io.IOException => {
     		logger.warn("connection to " + hostname + " failed")
				connection = new Connection(hostname)
				logger.info("Connecting to " + hostname)
				connection.connect()
				logger.info("Authenticating with username " + username + " privateKey " + privateKey)
				connection.authenticateWithPublicKey(username, privateKey, "")
				func(connection)
      }
			case e: java.net.SocketException => {
				logger.warn("connection to " + hostname + " failed")
				connection = new Connection(hostname)
				logger.info("Connecting to " + hostname)
				connection.connect()
				logger.info("Authenticating with username " + username + " privateKey " + privateKey)
				connection.authenticateWithPublicKey(username, privateKey, "")
				func(connection)
			}
		}
	}

	/**
	 * Execute a command sync and return the result as an ExecuteResponse
	 */
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
			var exitStatus:java.lang.Integer = null
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
							stdout.append("\n")
						}
					}
				}
				if((status & ChannelCondition.STDERR_DATA) != 0) {
					while(errReader.ready) {
						val line = errReader.readLine()
						if(line != null) {
							logger.debug("Received STDERR_DATA: " + line)
							stderr.append(line)
							stderr.append("\n")
						}
					}
				}
				if((status & ChannelCondition.EXIT_STATUS) != 0) {
					logger.debug("Received EXIT_STATUS")
					exitStatus = session.getExitStatus()
				}
				if((status & ChannelCondition.EXIT_SIGNAL) != 0) {
					logger.debug("Received EXIT_SIGNAL: " + session.getExitSignal())
				}
				if((status & ChannelCondition.EOF) != 0) {
					logger.debug("Received EOF")
				}
				if((status & ChannelCondition.CLOSED) != 0) {
					logger.debug("Received CLOSED")
					continue = false
				}
			}
			session.close()
			ExecuteResponse(Some(exitStatus.intValue), stdout.toString, stderr.toString)
		})
	}

	/**
	 * Create a file on the remote machine with the given contents
	 * @param file - The desired path of the file. If the given path is relative, the file will be created in the rootDirectory
	 * @param contents - A string with the desired contents of the file
	 */
	def createFile(file: File, contents: String): Unit = {
			useConnection((c) => {
				val session = connection.openSession
				session.execCommand("cat > " + file)
				session.getStdin().write(contents.getBytes)
				session.getStdin().close()
				session.close()
			})
	}

	/**
	 * Upload a file to the remote machine.  Before peforming the transfer check the md5hash of the local file and any existing file to ensure we don't waste bandwidth.
	 */
	def upload(localFile: File, remoteDirectory: File): Unit = {
		if(Util.md5(localFile) == md5(new File(remoteDirectory, localFile.getName)))
			logger.debug("Not uploading " + localFile + " as the hashes match")
		else
			useConnection((c) => {
				val scp = new SCPClient(connection)
        logger.debug("Hashes don't match, uploading " + localFile + " to " + remoteDirectory)
				scp.put(localFile.toString, remoteDirectory.toString)
        logger.debug("Upload of " + localFile + " complete")
			})
	}

	/**
	 * Downloads a file from the remote machine.
	 */
	def download(remoteFile: File, localDirectory: File): Unit = {
		val scp = new SCPClient(connection)
    logger.debug("Downloading " + remoteFile + " to " + localDirectory)
		scp.get(remoteFile.toString, localDirectory.toString)
    logger.debug("Transfer of " + remoteFile + " complete")
	}

	def md5(remoteFile: File): String = {
		val failureResponse = "md5sum: " + remoteFile + ": No such file or directory"
		executeCommand("md5sum " + remoteFile) match {
			case ExecuteResponse(Some(0), result, "") => {
        val hash = result.split(" ")(0)
        logger.debug("Got hash: " + hash)
        hash
      }
			case ExecuteResponse(Some(1),"", failureResponse) => {
        logger.warn("Hash command returned with an error")
        ""
      }
			case e: ExecuteResponse => {
				logger.fatal("Unexpected response while calculating md5: " + e)
				""
			}
		}
	}

	def catFile(remoteFile: File): String = {
		executeCommand("cat " + remoteFile) match {
			case ExecuteResponse(Some(0), data, "") => data
			case e: ExecuteResponse => {
				logger.fatal("Unexpected response during cat: " + e)
				""
			}
		}
	}

	def tail(remoteFile: File):String = {
		executeCommand("tail -n 20 " + remoteFile) match {
			case ExecuteResponse(Some(0), logTail, "") => logTail
			case e: ExecuteResponse => {
				logger.fatal("Unexpected response while tailing log: " + e)
				""
			}
		}
	}

	def mkdir(remoteDir: File): Unit = {
		executeCommand("mkdir -p " + remoteDir) match {
			case ExecuteResponse(Some(0),_, _) => true
			case _ => logger.fatal("Unexpected response while making directory " + remoteDir)
		}
	}

	def watch(remoteFile: File): Unit = {
		useConnection((c) => {
			val session = connection.openSession
			val outReader = new BufferedReader(new InputStreamReader(session.getStdout()))

			session.execCommand("tail -F " + remoteFile)

			val thread = new Thread("FileWatcher-" + hostname) {
				override def run() = {
					var line = outReader.readLine()
					while(line != null) {
						println(hostname + " " + line)
						line = outReader.readLine()
					}
					session.close()
				}
			}
			thread.setDaemon(true)
			thread.start
      logger.debug("Watching " + remoteFile + " on thread " + thread)
		})
	}

	def blockTillFileCreated(file: File): Unit = {
		useConnection((c) => {
			val session = connection.openSession
			session.execCommand("tail -F " + file)

			logger.debug("Blocking till " + file + "exists")

			var status = session.waitForCondition(ChannelCondition.STDOUT_DATA |
																						ChannelCondition.EXIT_STATUS |
																						ChannelCondition.EXIT_SIGNAL |
																						ChannelCondition.EOF |
																						ChannelCondition.CLOSED, 10000)

			while((status & ChannelCondition.STDOUT_DATA) == 0) {
				logger.info("Waiting for the creation of " + file + " on " + hostname)
				status = session.waitForCondition(ChannelCondition.STDOUT_DATA |
																						ChannelCondition.EXIT_STATUS |
																						ChannelCondition.EXIT_SIGNAL |
																						ChannelCondition.EOF |
																						ChannelCondition.CLOSED, 10000)
			}
		})
	}

	def blockTillPortOpen(port: Int): Unit = {
  	var connected = false

		while(!connected) {
			try {
				val s = new java.net.Socket(hostname, port)
				connected = true
			}
			catch {
				case ce: java.net.ConnectException => {
					logger.info("Connection to " + hostname + ":" + port + " failed, waiting 5 seconds")
				}
			}
			Thread.sleep(5000)
		}
	}

  def isPortAvailableToListen(port: Int): Boolean = {
        executeCommand("netstat -aln | grep -v unix | grep LISTEN | egrep '\\b" + port + "\\b'") match {
			case ExecuteResponse(Some(_), result, "") => {
                result.trim.isEmpty
            }
			case e: ExecuteResponse => {
				logger.fatal("Unexpected response while executing netstat: " + e)
				false
			}
        }
    }

  def stopWatches(): Unit = {
    executeCommand("killall tail")
  }


	def ls(dir: File):Seq[RemoteFile] = {
		executeCommand("ls -lh " + dir) match {
			case ExecuteResponse(Some(0), data, "") => {
				data.split("\n").drop(1).map(l => {
					val parts = l.split(" ")
					RemoteFile(parts(7), parts(2), parts(0), parts(5) + parts(6), parts(4))
				})
			}
			case er => throw new UnknownResponse(er)
		}
	}

	override def toString(): String = "<RemoteMachine " + username + "@" + hostname + ">"
}
