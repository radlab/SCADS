import ch.ethz.ssh2.Connection
import ch.ethz.ssh2.Session
import ch.ethz.ssh2.StreamGobbler

import java.io.File
import java.io.IOException

class SSH(hostname: String, keyPath: String){
  val connection = new Connection(hostname)
  
  def executeCommand(cmd: String): Array[String] = {
    connect
    val output = Array("","")
    
    logout
    
    output
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