package deploylib
package mesos

import ec2._
import config._

import java.io.File
import java.net.InetAddress

object MesosEC2 extends ConfigurationActions {
  val rootDir = new File("/root/mesos/frameworks/deploylib")
  val masterAddress = InetAddress.getByName("mesos-ec2.knowsql.org")

  def updateDeploylib: Unit = {
    val executorScript = Util.readFile(new File("src/main/resources/java_executor"))
    EC2Instance.activeInstances.pforeach(inst => {
      createDirectory(inst, rootDir)
      uploadFile(inst, new File("target/deploy-2.1-SNAPSHOT-jar-with-dependencies.jar"), rootDir)
      createFile(inst, new File(rootDir, "java_executor"), executorScript, "755")
    })
  }

  //TODO: security groups would be better...
  def slaves = EC2Instance.activeInstances.filterNot(i => masterAddress.getHostAddress equals InetAddress.getByName(i.publicDnsName).getHostAddress)
  def master = EC2Instance.activeInstances.find(i => masterAddress.getHostAddress equals InetAddress.getByName(i.publicDnsName).getHostAddress).get

  def updateClusterUrl: Unit = {
    val location = new File("/root/mesos-ec2/cluster-url")
    val contents = "1@" + master.privateDnsName + ":5050"
    master ! ("hostname " + master.privateDnsName)
    createFile(master, location, contents, "644")
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

  def restart: Unit = {
    updateSlavesFile
    updateMasterFile
    updateClusterUrl
    master ! "/root/mesos-ec2/stop-mesos"
    master ! "/root/mesos-ec2/start-mesos"
  }

  def addSlaves(count: Int): Unit = {
    EC2Instance.runInstances(
      "ami-f8806a91",
      count,
      count,
      EC2Instance.keyName,
      "m1.large",
      "us-east-1b")
    EC2Instance.activeInstances.pforeach(_ ! "mkdir -p /mnt/mesos-logs/")
    updateDeploylib
    logger.info("deploying mesos to new machines")
    master ! "/root/mesos-ec2/redeploy-mesos"
    master ! "rm /root/.ssh/known_hosts"
    restart
  }

}
