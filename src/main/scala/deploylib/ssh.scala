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

class SSH(hostname: String, keyPath: String){
  val connection = new Connection(hostname)
  
  def executeCommand(cmd: String): ExecuteResponse = {
    connect

    val stdout = new StringBuilder
    val stderr = new StringBuilder

    val session = connection.openSession()
    
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
    
    
    val response = new ExecuteResponse(session.getExitStatus(),
                                       stdout.toString,
                                       stderr.toString)
    
    session.close()
    logout
    
    response
  }
  
  def upload(localPath: String, remotePath: String) = {
    connect
    val scp = new SCPClient(connection)
    scp.put(localPath, remotePath)
    logout
  }
  
  private def connect = {
    val keyfile = new File(keyPath)
    
    connection.connect()
    
    if (connection.authenticateWithPublicKey("root", keyfile, "") == false) {
      throw new IOException("Authentication failed.")
    }
  }
  
  private def logout = {
    connection.close()
  }
}