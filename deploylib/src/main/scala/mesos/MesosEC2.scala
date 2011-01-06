package deploylib
package mesos

import ec2._
import config._

import java.io.File
import java.net.InetAddress

object MesosEC2 extends ConfigurationActions {
  val rootDir = new File("/usr/local/mesos/frameworks/deploylib")

  def updateDeploylib: Unit = {
    val executorScript = Util.readFile(new File("src/main/resources/java_executor"))
    slaves.pforeach(inst => {
      createDirectory(inst, rootDir)
      uploadFile(inst, new File("target/deploy-2.1-SNAPSHOT-jar-with-dependencies.jar"), rootDir)
      createFile(inst, new File(rootDir, "java_executor"), executorScript, "755")
    })
  }

  val masterTag = "mesosMaster"
  def slaves = EC2Instance.activeInstances.pfilterNot(_.tags contains masterTag)
  def master = EC2Instance.activeInstances.pfilter(_.tags contains masterTag).head

  def clusterUrl = "1@" + master.privateDnsName + ":5050"

  def restartSlaves: Unit = {
    slaves.pforeach(_ ! "service mesos-slave stop")
    slaves.pforeach(_ ! "service mesos-slave start")
  }

  def restartMaster: Unit = {
    master ! "service mesos-master stop"
    master ! "service mesos-master start"
  }

  def restart: Unit = {
    restartMaster
    restartSlaves
  }

  def addSlaves(count: Int): Unit = {
    val userData = try Some("url=" + clusterUrl) catch {
      case noMaster: java.util.NoSuchElementException =>
	logger.warning("No master found. Starting without userdata")
	None
    }

    EC2Instance.runInstances(
      "ami-58798d31",
      count,
      count,
      EC2Instance.keyName,
      "m1.large",
      "us-east-1b",
      userData)
  }

}
