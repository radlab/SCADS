package deploylib
package mesos

import ec2._
import config._

import java.io.File

object MesosEC2 extends ConfigurationActions {
  def updateDeploylib: Unit = {
    val executorScript = Util.readFile(new File("src/main/resources/java_executor"))
    EC2Instance.activeInstances.pforeach(inst => {
      inst.upload(new File("target/deploy-2.1-SNAPSHOT-jar-with-dependencies.jar"), new File("/root"))
      createFile(inst, new File("/root/java_executor"), executorScript, "755")
    })
  }
}
