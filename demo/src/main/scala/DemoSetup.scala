package edu.berkeley.cs
package radlab

import scads.comm._
import deploylib.mesos._

object DemoConfig {
  val javaExecutorPath = "/usr/local/mesos/frameworks/deploylib/java_executor"
  def localMesosMasterPid = "1@" + java.net.InetAddress.getLocalHost.getHostName + ":5050"
  def mesosMaster = new String(DemoZooKeeper.root("demo/mesosMaster").data)

  def serviceSchedulerNode = DemoZooKeeper.root.getOrCreate("demo/serviceScheduler")
  //def serviceScheduler = classOf[RemoteActor].newInstance.parse(serviceSchedulerNode.data)
  def serviceScheduler = RemoteActor("ec2-50-16-98-186.compute-1.amazonaws.com", 9000, ActorNumber(0))

  def scadrWar = "http://s3.amazonaws.com/deploylibCache-trush/b23b2004470821b434cb71cd6321f69c"

  val jdbcDriver = classOf[com.mysql.jdbc.Driver]
  val dashboardDb = "jdbc:mysql://dev-mini-demosql.cwppbyvyquau.us-east-1.rds.amazonaws.com:3306/radlabmetrics?user=radlab_dev&password=randyAndDavelab"
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

object WebAppScheduler extends optional.Application {
  import DemoConfig._

  def main(name: String, mesosMaster: String, executor: String, warFile: String): Unit = {
    System.loadLibrary("mesos")
    new WebAppScheduler(name, mesosMaster, executor, S3CachedJar(warFile), 1, Some(dashboardDb))
  }
}

object Demo {
  import DemoConfig._

  def startScadr: Unit = {
    serviceScheduler !? RunExperiment(
      JvmMainTask(MesosEC2.classSource,
		  "edu.berkeley.cs.radlab.WebAppScheduler",
		  "--name" :: "SCADr" ::
		  "--mesosMaster" :: MesosEC2.clusterUrl ::
		  "--executor" :: javaExecutorPath ::
		  "--warFile" :: scadrWar :: Nil) :: Nil
    )
  }
}
