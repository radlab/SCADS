package deploylib
package mesos

import ec2._
import config._

import java.io.File

object MesosEC2 extends ConfigurationActions {
  val rootDir = new File("/root/mesos/frameworks/deploylib")
  def updateDeploylib: Unit = {
    val executorScript = Util.readFile(new File("src/main/resources/java_executor"))
    EC2Instance.activeInstances.pforeach(inst => {
      createDirectory(inst, rootDir)
      uploadFile(inst, new File("target/deploy-2.1-SNAPSHOT-jar-with-dependencies.jar"), rootDir)
      createFile(inst, new File(rootDir, "java_executor"), executorScript, "755")
    })
  }
}
