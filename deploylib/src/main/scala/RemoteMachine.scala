package deploylib

import java.io.{File, BufferedReader, InputStreamReader}
import net.lag.logging.Logger
import ch.ethz.ssh2.{Connection, Session, ChannelCondition, SCPClient}

import scala.collection.generic.SeqForwarder

/**
 * Holds the results of the execution of a command on the remote machine.
 * @param status - the integer status code returned by the command if one was returned, otherwise None
 * @param stdout - a string with all of the data that was output on STDOUT by the command
 * @param stderr - a string with all of the data that was output on STDERR by the command
 */
case class ExecuteResponse(status: Option[Int], stdout: String, stderr: String)

case class UnknownResponse(er: ExecuteResponse) extends Exception

/**
 * A trait that allows remote machine to maintain a list of 'tags' that are stored
 * in a file in the user home directory on the remote machine.
 */
trait Taggable {
  self: RemoteMachine =>

  val tagFile = new File("~/.deploylib_tags")

  object tags extends SeqForwarder[String] {
    private def getTags: Seq[String] = {
      self ! ("touch " + tagFile)
      catFile(tagFile).split("\n").filterNot(_ equals "")
    }

    private def setTags(tags: Set[String]): Unit =
      createFile(tagFile, tags.mkString("\n"))

    def underlying = getTags

    def +=(tag: String): Unit =
      setTags(((getTags :+ tag).toSet))

    def -=(tag: String): Unit =
      setTags(getTags.filterNot(_ equals tag).toSet)
  }

}

trait Sudo extends RemoteMachine {
  protected override def prepareCommand(cmd: String) = "sudo bash -c '%s'".format(cmd)
}

trait ServiceManager extends RemoteMachine {
  self =>

  val serviceDir = new File("$HOME/deploylib/services")
  val logDir = new File("$HOME/deploylib/logs")

  case class RemoteService(name: String) {
    val runScript = new java.io.File(serviceDir, name + ".sh")
    val pidFile = new java.io.File(serviceDir, name + ".pid")
    val logFile = new java.io.File(logDir, name + ".log")

    def setCmd(cmd: String): this.type = {
      mkdir(serviceDir)
      mkdir(logDir)
      val serviceScript = "#!/bin/bash\n" + cmd
      createFile(runScript, serviceScript)
      self ! ("chmod 755 " + runScript)
      this
    }

    def start = self ! "start-stop-daemon --make-pidfile --start --background --pidfile %s --exec %s >> %s 2>&1".format(pidFile, runScript, logFile)
    def stop = self.executeCommand("start-stop-daemon --stop --pidfile %s".format(pidFile))
    def restart = {stop; start}
  }
  def getService(name: String, cmd: String) = RemoteService(name).setCmd(cmd)
}

/**
 * Provides a framework for interacting (running commands, uploading/downloading files, etc) with a generic remote machine.
 */
abstract class RemoteMachine {
  self =>

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
  val privateKey: File = findPrivateKey

  protected def findPrivateKey: File = {
    def defaultKeyFiles = {
      val specifiedKey = System.getenv("DEPLOYLIB_SSHKEY")
      val rsa = new File(System.getProperty("user.home"), ".ssh/id_rsa")
      val dsa = new File(System.getProperty("user.home"), ".ssh/id_dsa")

      if (specifiedKey != null) new File(specifiedKey)
      else if (rsa.exists) rsa
      else if (dsa.exists) dsa
      else throw new RuntimeException("No private key found")
    }
    Config.getString("deploylib.private_key") match {
      case Some(pk) =>
        val f = new File(pk)
        if (f.exists) f
        else defaultKeyFiles
      case None => defaultKeyFiles
    }
  }

  /**
   * The default root directory for operations (The user should have read write permissions to this directory)
   */
  val rootDirectory: File

  /**
   * The location of the java command on the remote machine
   */
  val javaCmd: File

  /**
   * The services that are assigned to be deployed to this remote machine.
   */
  protected var assignedServices: Set[Service] = Set()

  def addService(service: Service): Unit = {
    assignedServices += service
  }

  val runitBinaryPath: File
  lazy val runsvdirCmd: File = new File(runitBinaryPath, "runsvdir")
  lazy val svCmd: File = new File(runitBinaryPath, "sv")
  lazy val svlogdCmd: File = new File(runitBinaryPath, "svlogd")
  lazy val serviceRoot = new File(rootDirectory, "services")

  val logger = Logger()
  private var connection: Connection = null

  implicit def toOption[A](a: A) = Option(a)

  protected def prepareCommand(cmd: String) = cmd

  /**
   * Provide an ssh connection to the server.  If one is not already available or has been disconnected, create one.
   */
  protected def useConnection[ReturnType](func: (Connection) => ReturnType, numTries: Int = 5): ReturnType = {
    def onFailure(e: Exception) = {
      connection = null
      logger.warning("Connection to " + hostname + " failed: %s", e)
      if (numTries <= 1)
        throw new RuntimeException("Number of tries exceeded" + e)
      Thread.sleep(30 * 1000)
      useConnection(func, numTries - 1)
    }

    try {
      if (connection == null) synchronized {
        if (connection == null) {
          connection = new Connection(hostname)
          logger.info("Connecting to " + hostname)
          connection.connect()
          logger.info("Authenticating with username " + username + " privateKey " + privateKey)
          connection.authenticateWithPublicKey(username, privateKey, "")
        }
      }
      func(connection)
    } catch {
      case e: java.io.IOException => onFailure(e)
      case e: java.net.SocketException => onFailure(e)
      case e: java.lang.IllegalStateException => onFailure(e)
    }
  }

  /**
   * Executes a command and throw an error if it doesn't return 0
   */
  def !(cmd: String) = executeCommand(cmd) match {
    case ExecuteResponse(Some(0), stdout, stderr) => logger.debug("===stdout %s: %s===\n%s===stderr %s:%s===%s\n", hostname, cmd, stdout, hostname, cmd, stderr)
    case ExecuteResponse(_, stdout, stderr) => {
      logger.warning("===stdout %s: %s===\n%s===stderr %s:%s===%s\n", hostname, cmd, stdout, hostname, cmd, stderr)
      throw new RuntimeException("Cmd: " + cmd + " failed")
    }
  }

  /**
   * Execute a command on the remote machine and return stdout as a string.  Throws an exception if the command doesn't exit with status code 0.
   */
  def !?(cmd: String) = executeCommand(cmd) match {
    case ExecuteResponse(Some(0), stdout, "") => stdout
    case err => throw new UnknownResponse(err)
  }

  /**
   * Execute a command sync with a maximum timeout to wait for result
   * and return the result as an ExecuteResponse
   */
  def executeCommand(cmd: String, timeout: Long = 0): ExecuteResponse = {
    useConnection((c) => {
      val stdout = new StringBuilder
      val stderr = new StringBuilder

      val session = connection.openSession
      val outReader = new BufferedReader(new InputStreamReader(session.getStdout()))
      val errReader = new BufferedReader(new InputStreamReader(session.getStderr()))

      logger.debug("Executing: " + cmd)
      session.execCommand(prepareCommand(cmd))

      var continue = true
      var exitStatus: java.lang.Integer = null
      while (continue) {
        val status = session.waitForCondition(ChannelCondition.STDOUT_DATA |
          ChannelCondition.STDERR_DATA |
          ChannelCondition.EXIT_STATUS |
          ChannelCondition.EXIT_SIGNAL |
          ChannelCondition.EOF |
          ChannelCondition.CLOSED |
          ChannelCondition.TIMEOUT, timeout)

        if ((status & ChannelCondition.STDOUT_DATA) != 0) {
          while (outReader.ready) {
            val line = outReader.readLine()
            if (line != null) {
              logger.debug("Received STDOUT_DATA: " + line)
              stdout.append(line)
              stdout.append("\n")
            }
          }
        }
        if ((status & ChannelCondition.STDERR_DATA) != 0) {
          while (errReader.ready) {
            val line = errReader.readLine()
            if (line != null) {
              logger.debug("Received STDERR_DATA: " + line)
              stderr.append(line)
              stderr.append("\n")
            }
          }
        }
        if ((status & ChannelCondition.EXIT_STATUS) != 0) {
          logger.debug("Received EXIT_STATUS")
          exitStatus = session.getExitStatus()
        }
        if ((status & ChannelCondition.EXIT_SIGNAL) != 0) {
          logger.debug("Received EXIT_SIGNAL: " + session.getExitSignal())
        }
        if ((status & ChannelCondition.EOF) != 0) {
          logger.debug("Received EOF")
        }
        if ((status & ChannelCondition.CLOSED) != 0) {
          logger.debug("Received CLOSED")
          continue = false
        }
        if ((status & ChannelCondition.TIMEOUT) != 0) {
          logger.debug("Received TIMEOUT")
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
      logger.debug("Creating file %s: %s %s", file, contents, prepareCommand("cat > " + file))
      val session = connection.openSession
      session.execCommand(prepareCommand("cat > " + file))
      session.getStdin().write(contents.getBytes)
      session.getStdin().close()
      session.close()
    })
  }

  /**
   * Append to file on the remote machine with the given contents
   * @param file - The desired path of the file. If the given path is relative, the file will be created in the rootDirectory
   * @param contents - A string to be appended to the file
   */
  def appendFile(file: File, contents: String): Unit = {
    useConnection((c) => {
      val session = connection.openSession
      session.execCommand(prepareCommand("cat >> " + file))
      session.getStdin().write(contents.getBytes)
      session.getStdin().close()
      session.close()
    })
  }


  /**
   * Upload a file to the remote machine.  Before performing the transfer check the md5hash of the local file and any existing file to ensure we don't waste bandwidth.
   */
  def upload(localFile: File, remoteDirectory: File): Unit = {
    if (Util.md5(localFile) == md5(new File(remoteDirectory, localFile.getName)))
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

  /**
   * returns an md5 hash of the specified remote file
   */
  def md5(remoteFile: File): String = {
    val failureResponse = "md5sum: " + remoteFile + ": No such file or directory"
    executeCommand("md5sum " + remoteFile) match {
      case ExecuteResponse(Some(0), result, "") => {
        val hash = result.split(" ")(0)
        logger.debug("Got hash: " + hash)
        hash
      }
      case ExecuteResponse(Some(1), "", failureResponse) => {
        logger.debug("Hash command returned with an error")
        ""
      }
      case e: ExecuteResponse => {
        logger.fatal("Unexpected response while calculating md5: " + e)
        ""
      }
    }
  }

  /**
   * Return the contents of the remote file as a string.
   */
  def catFile(remoteFile: File): String = {
    executeCommand("cat " + remoteFile) match {
      case ExecuteResponse(Some(0), data, "") => data
      case e: ExecuteResponse => {
        logger.fatal("Unexpected response during cat: " + e)
        ""
      }
    }
  }

  /**
   * return the last numLines from the specified file
   */
  def tail(remoteFile: File, numLines: Int = 20): String = {
    executeCommand("tail -n %d ".format(numLines) + remoteFile) match {
      case ExecuteResponse(Some(0), logTail, "") => logTail
      case e: ExecuteResponse => {
        logger.fatal("Unexpected response while tailing log: " + e)
        ""
      }
    }
  }

  /**
   * create directories on the remote machine.
   */
  def mkdir(remoteDir: File): Unit = {
    executeCommand("mkdir -p " + remoteDir) match {
      case ExecuteResponse(Some(0), _, _) => true
      case _ => logger.fatal("Unexpected response while making directory " + remoteDir)
    }
  }

  /**
   * run tail -F on the specifed file in a seperate thread.  all ouput is printed on local stdout.
   */
  def watch(remoteFile: File): Unit = {
    useConnection((c) => {
      val session = connection.openSession
      val outReader = new BufferedReader(new InputStreamReader(session.getStdout()))

      session.execCommand(prepareCommand("tail -F " + remoteFile))

      val thread = new Thread("FileWatcher-" + hostname) {
        override def run() = {
          var line = outReader.readLine()
          while (line != null) {
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

  /**
   * Block until the specified file is created on the remote machine.
   */
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

      while ((status & ChannelCondition.STDOUT_DATA) == 0) {
        logger.info("Waiting for the creation of " + file + " on " + hostname)
        status = session.waitForCondition(ChannelCondition.STDOUT_DATA |
          ChannelCondition.EXIT_STATUS |
          ChannelCondition.EXIT_SIGNAL |
          ChannelCondition.EOF |
          ChannelCondition.CLOSED, 10000)
      }
    })
  }

  /**
   * Block until we can make a connection to the given port, checking every 5 seconds.
   */
  def blockTillPortOpen(port: Int): Unit = {
    var connected = false

    while (!connected) {
      try {
        val s = new java.net.Socket(hostname, port)
        connected = true
      } catch {
        case ce: java.net.ConnectException => {
          logger.info("Connection to " + hostname + ":" + port + " failed, waiting 5 seconds")
        }
      }
      Thread.sleep(5000)
    }
  }

  /**
   * Use netstat to check if a port is currently bound to a listening socket on the remote machine.
   */
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

  /**
   * Kill all processes currently tailing a file.
   */
  def stopWatches(): Unit = {
    executeCommand("killall tail")
  }

  case class RemoteFile(name: String, owner: String, permissions: String, modDate: String, size: String)

  /**
   * Return a list of RemoteFiles found in dir on the remote machine.
   */
  def ls(dir: File): Seq[RemoteFile] = {
    executeCommand("ls -lh " + dir) match {
      case ExecuteResponse(Some(0), data, "") => {
        data.split("\n").drop(1).map(l => {
          val parts = l.split(" ").flatMap {
            case "" => Nil;
            case x => Some(x)
          }
          RemoteFile(parts(7), parts(2), parts(0), parts(5) + parts(6), parts(4))
        })
      }
      case er => throw new UnknownResponse(er)
    }
  }

  case class RemoteJavaProcess(pid: Int, main: String) {
    val remoteMachine = self

    /**
     * Return a string containing a stack dump for all the threads.
     */
    def stack = self !? ("jstack " + pid)
  }

  /**
   * Return a list of all the java processes running on the remote machine.
   */
  def jps: Seq[RemoteJavaProcess] = {
    val javaProcessRegEx = """(\d+) (\S+)""".r
    executeCommand("jps") match {
      case ExecuteResponse(Some(0), out, "") => {
        out.split("\n").flatMap {
          case javaProcessRegEx(pid, "jps") => None
          case javaProcessRegEx(pid, main) => Some(new RemoteJavaProcess(pid.toInt, main))
        }
      }
      case er => throw new UnknownResponse(er)
    }
  }

  case class RemoteProcess(user: String, pid: Int, cpu: Float, mem: Float, vsz: Int, rss: Int, tty: String, stat: String, start: String, time: String, command: String)

  /**
   * lists the processes running on the remote machine.
   * Expects:
   * USER       PID %CPU %MEM    VSZ   RSS TTY      STAT START   TIME COMMAND
   * root         1  0.0  0.0  23708  1952 ?        Ss   01:00   0:00 /sbin/init
   */
  def ps: Seq[RemoteProcess] =
    (this !? "ps aux").split("\n").drop(1)
      .map(_.split("\\s+"))
      .map(rp => RemoteProcess(rp(0),
      rp(1).toInt,
      rp(2).toFloat,
      rp(3).toFloat,
      rp(4).toInt,
      rp(5).toInt,
      rp(6),
      rp(7),
      rp(8),
      rp(9),
      rp(10)))



  case class FreeStats(total: Int, used: Int, free: Int, shared: Int, buffers: Int, cached: Int)
  val freeResponse = """.*Mem:\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+).*""".r
  /**
   * Returns the result of the command free.
   *
   * Parsed as:
   * total       used       free     shared    buffers     cached
   * Mem:       7864548    4332752    3531796          0      78112    2640248
   */
  def free = (this !? "free -m").split("\n").flatMap {
    case freeResponse(total, used, freeMem, shared, buffers, cached) =>
      FreeStats(total.toInt, used.toInt, freeMem.toInt, shared.toInt, buffers.toInt, cached.toInt) :: Nil
    case _ => Nil
  }.head


  override def toString(): String = "<RemoteMachine " + username + "@" + hostname + ">"
}
