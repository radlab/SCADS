package edu.berkeley.cs
package radlab
package demo

import avro.marker._
import avro.runtime._

import scads.comm._
import deploylib.mesos._

object DemoConfig {
  val javaExecutorPath = "/usr/local/mesos/frameworks/deploylib/java_executor"
  def localMesosMasterPid = "1@" + java.net.InetAddress.getLocalHost.getHostName + ":5050"
  def mesosMaster = new String(DemoZooKeeper.root("demo/mesosMaster").data)

  def serviceSchedulerNode = DemoZooKeeper.root.getOrCreate("demo/serviceScheduler")
  def serviceScheduler = classOf[RemoteActor].newInstance.parse(serviceSchedulerNode.data)

  def scadrWar = S3CachedJar("http://s3.amazonaws.com/deploylibCache-trush/b23b2004470821b434cb71cd6321f69c")

  val jdbcDriver = classOf[com.mysql.jdbc.Driver]
  val dashboardDb = "jdbc:mysql://dev-mini-demosql.cwppbyvyquau.us-east-1.rds.amazonaws.com:3306/radlabmetrics?user=radlab_dev&password=randyAndDavelab"

  implicit def classSource = MesosEC2.classSource
}

//TODO: Add other zookeeper
object DemoZooKeeper extends ZooKeeperProxy("ec2-50-16-2-36.compute-1.amazonaws.com,ec2-174-129-105-138.compute-1.amazonaws.com")

object ServiceSchedulerDaemon extends optional.Application {
  import DemoConfig._

  def main(mesosMaster: Option[String]): Unit = {
    System.loadLibrary("mesos")
    val scheduler = new ServiceScheduler(
      mesosMaster.getOrElse(DemoConfig.localMesosMasterPid),
      javaExecutorPath
    )
    serviceSchedulerNode.data = scheduler.remoteHandle.toBytes
  }
}


//TODO: Move to web app scheduler.
case class WebAppSchedulerTask(var name: String, var mesosMaster: String, var executor: String, var warFile: String) extends AvroTask with AvroRecord {
  import DemoConfig._

  def run(): Unit = {
    System.loadLibrary("mesos")
    new WebAppScheduler(name, mesosMaster, executor, S3CachedJar(warFile), 1, Some(dashboardDb))
  }
}
