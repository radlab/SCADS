package deploylib
package mesos

import ec2._
import config._

import java.io.File
import java.net.InetAddress

object MesosEC2 extends ConfigurationActions {
  val rootDir = new File("/usr/local/mesos/frameworks/deploylib")
  val masterAddress = InetAddress.getByName("mesos-ec2.knowsql.org")

  def updateDeploylib: Unit = {
    val executorScript = Util.readFile(new File("src/main/resources/java_executor"))
    slaves.pforeach(inst => {
      createDirectory(inst, rootDir)
      uploadFile(inst, new File("target/deploy-2.1-SNAPSHOT-jar-with-dependencies.jar"), rootDir)
      createFile(inst, new File(rootDir, "java_executor"), executorScript, "755")
    })
  }

  //TODO: security groups would be better...
  def slaves = EC2Instance.activeInstances.filterNot(i => masterAddress.getHostAddress equals InetAddress.getByName(i.publicDnsName).getHostAddress)
  def master = EC2Instance.activeInstances.find(i => masterAddress.getHostAddress equals InetAddress.getByName(i.publicDnsName).getHostAddress).get

  def clusterUrl = "1@" + master.privateDnsName + ":5050"

  def updateClusterUrl: Unit = {
    val location = new File("/root/mesos-ec2/cluster-url")
    master ! ("hostname " + master.privateDnsName)
    createFile(master, location, clusterUrl, "644")
  }

  def updateMasterFile: Unit = {
    val location = new File("/root/mesos-ec2/masters")
    val contents = master.privateDnsName
    createFile(master, location, contents, "644")
  }

  def updateSlavesFile: Unit = {
    val location = new File("/root/mesos-ec2/slaves")
    val contents = slaves.map(_.privateDnsName).mkString("\n")
    createFile(master, location, contents, "644")
  }

  def restartSlaves: Unit = {
    slaves.pforeach(_ ! "service mesos-slave stop")
    slaves.pforeach(_ ! "service mesos-slave start")
  }

  def restart: Unit = {
    updateSlavesFile
    updateMasterFile
    updateClusterUrl
    master ! "/root/mesos-ec2/stop-mesos"
    master ! "/root/mesos-ec2/start-mesos"
  }

  def addSlaves(count: Int): Unit = {
    EC2Instance.runInstances(
      "ami-58798d31",
      count,
      count,
      EC2Instance.keyName,
      "m1.large",
      "us-east-1b",
      Some("url=" + clusterUrl))
  }

}
