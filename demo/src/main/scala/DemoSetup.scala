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

  //TODO: Add other ZooKeeper
  val zooKeeperRoot = ZooKeeperNode("zk://ec2-50-16-2-36.compute-1.amazonaws.com,ec2-174-129-105-138.compute-1.amazonaws.com/demo")
  def scadrRoot =  zooKeeperRoot.getOrCreate("apps/scadr")

  val mesosMasterNode = zooKeeperRoot.getOrCreate("mesosMaster")
  def mesosMaster = new String(mesosMasterNode.data)

  def serviceSchedulerNode = zooKeeperRoot.getOrCreate("serviceScheduler")
  def serviceScheduler = classOf[RemoteActor].newInstance.parse(serviceSchedulerNode.data)

  def scadrWar = S3CachedJar("http://s3.amazonaws.com/deploylibCache-trush/b23b2004470821b434cb71cd6321f69c")

  val jdbcDriver = classOf[com.mysql.jdbc.Driver]
  val dashboardDb = "jdbc:mysql://dev-mini-demosql.cwppbyvyquau.us-east-1.rds.amazonaws.com:3306/radlabmetrics?user=radlab_dev&password=randyAndDavelab"

  implicit def classSource = MesosEC2.classSource
}

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
case class WebAppSchedulerTask(var name: String, var mesosMaster: String, var executor: String, var warFile: S3CachedJar) extends AvroTask with AvroRecord {
  import DemoConfig._

  def run(): Unit = {
    System.loadLibrary("mesos")
    val scheduler = new WebAppScheduler(name, mesosMaster, executor, warFile, 1, Some(dashboardDb))
    scheduler.monitorThread.join()
  }
}

object IntKeyScaleScheduler extends optional.Application {
  import DemoConfig._

  def main(name: String, mesosMaster: String, executor: String, cp: String): Unit = {
    System.loadLibrary("mesos")
    implicit val scheduler = LocalExperimentScheduler(name, mesosMaster, executor)
    implicit val classpath = cp.split(":").map(S3CachedJar(_)).toSeq
    implicit val zookeeper = zooKeeperRoot.getOrCreate("scads/perf") 

    import scads.perf.intkey._
    val cluster = DataLoader(1,1).newCluster
    RandomLoadClient(1, 1).schedule(cluster)
  }
}

object RepTestScheduler extends optional.Application {
  import DemoConfig._

  def main(name: String, mesosMaster: String, executor: String, cp: String): Unit = {
    System.loadLibrary("mesos")
    implicit val scheduler = LocalExperimentScheduler(name, mesosMaster, executor)
    implicit val classpath = cp.split(":").map(S3CachedJar(_)).toSeq
    implicit val zookeeper = zooKeeperRoot.getOrCreate("scads/perf") 

    import scads.perf.reptest._
    val cluster = DataLoader(1, 1000).newCluster
    RepClient(3).schedule(cluster)

    zookeeper.awaitChild("expFinished")
    val loadResults = cluster.getNamespace[RepResultKey, RepResultValue]("repResults")
    loadResults.getRange(None, None).foreach(println)
  }
}
