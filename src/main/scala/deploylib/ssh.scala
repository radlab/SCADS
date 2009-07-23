package deploylib

import ch.ethz.ssh2.Connection
import ch.ethz.ssh2.Session
import ch.ethz.ssh2.StreamGobbler
import ch.ethz.ssh2.SCPClient

import java.io.File
import java.io.IOException
import java.io.InputStream
import java.io.InputStreamReader
import java.io.BufferedReader

class SSH(hostname: String){
  val connection = new Connection(hostname)
  
  def executeCommand(cmd: String): ExecuteResponse = {
    var response: ExecuteResponse = null
    try {
      connect

      val stdout = new StringBuilder
      val stderr = new StringBuilder

      val session = connection.openSession()

      try {
        session.execCommand(cmd)
    
        val stdoutGobbler = new StreamGobbler(session.getStdout())
        val stderrGobbler = new StreamGobbler(session.getStderr())
    
        val stdoutReader = new BufferedReader(new InputStreamReader(stdoutGobbler))
        val stderrReader = new BufferedReader(new InputStreamReader(stderrGobbler))
    
        var stdoutLine = stdoutReader.readLine()
        while (stdoutLine != null) {
          stdout.append(stdoutLine + "\n")
          stdoutLine = stdoutReader.readLine()
        }
    
        var stderrLine = stderrReader.readLine()
        while (stderrLine != null) {
          stderr.append(stderrLine + "\n")
          stderrLine = stderrReader.readLine()
        }
    
    
        response = new ExecuteResponse(session.getExitStatus(),
                                           stdout.toString,
                                           stderr.toString)
      } finally {
        session.close()
      }
    } finally {
      logout
    }
    
    response
  }
  
  def upload(localFiles: Array[String], remoteDirectory: String) = {
    try {
      connect
      val scp = new SCPClient(connection)
      scp.put(localFiles, remoteDirectory)
    } finally {
      logout
    }
  }
  
  def download(remoteFiles: Array[String], localDirectory: String) = {
    try {
      connect
      val scp = new SCPClient(connection)
      scp.get(remoteFiles, localDirectory)
    } finally {
      logout
    }
  }
  
  private def connect = {
    require(DataCenter.keyPath != null,
      "DataCenter.keyPath must be set either directly " + 
      "or by setting AWS_KEY_PATH environment variables before " +
      "calling this method.")
    val keyfile = new File(DataCenter.keyPath)
    val numConnectionAttempts = 5
    
    def connectAgain(numAttempts: Int): Unit = {
      try {
        connection.connect()
      } catch {
        case ex: IOException => numAttempts match {
          case 0 => throw new IOException("Tried " + numConnectionAttempts + 
            " times, but got this error:\n" + ex.getMessage())
          case _ => Thread.sleep(1000); connectAgain(numAttempts - 1)
        }
      }
    }

    connectAgain(numConnectionAttempts)
    
    if (connection.authenticateWithPublicKey("root", keyfile, "") == false) {
      throw new IOException("Authentication failed.")
    }
  }
  
  private def logout = {
    connection.close()
  }
}