package deploylib.physical

import java.io.File

import scala.collection.jcl.Conversions._

object Physical {
  var keyName = "marmbrus.key"
  var keyPath = "/Users/marmbrus/.ec2/amazon/"
}

class PhysicalInstance(val hostname: String) extends RemoteMachine {
  val username: String = "root"
  val privateKey: File = new File(keyPath + keyName)
  val rootDirectory: File = new File("/mnt/")
  val runitBinaryPath:File = new File("/usr/bin")
}
