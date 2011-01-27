package edu.berkeley.cs
package radlab
package demo

import avro.marker._
import avro.runtime._

import scads.comm._
import deploylib.mesos._
import deploylib.ec2._

import java.io.File

object DemoConfig {
  val javaExecutorPath = "/usr/local/mesos/frameworks/deploylib/java_executor"
  def localMesosMasterPid = "1@" + java.net.InetAddress.getLocalHost.getHostName + ":5050"

  //TODO: Add other ZooKeeper
  val zooKeeperRoot = ZooKeeperNode("zk://ec2-50-16-2-36.compute-1.amazonaws.com,ec2-174-129-105-138.compute-1.amazonaws.com/home/kcurtis")
  def scadrRoot =  zooKeeperRoot.getOrCreate("apps/scadr")
  def scadrWebServerList = scadrRoot.getOrCreate("webServerList")

  def traceRoot = zooKeeperRoot.getOrCreate("traceCollection")

  val mesosMasterNode = zooKeeperRoot.getOrCreate("mesosMaster")
  def mesosMaster = new String(mesosMasterNode.data)

  def serviceSchedulerNode = zooKeeperRoot.getOrCreate("serviceScheduler")
  def serviceScheduler = classOf[RemoteActor].newInstance.parse(serviceSchedulerNode.data)

  def scadrWar = S3CachedJar("http://s3.amazonaws.com/deploylibCache-andyk/cf4795cea32f45694ab018ebdf069a39")

  val jdbcDriver = classOf[com.mysql.jdbc.Driver]
  val dashboardDb = "jdbc:mysql://dev-mini-demosql.cwppbyvyquau.us-east-1.rds.amazonaws.com:3306/radlabmetrics?user=radlab_dev&password=randyAndDavelab"

  def rainJars = {
    val rainLocation  = new File("../rain-workload-toolkit")
    val rainJar = new File(rainLocation, "rain.jar")
    val scadrJar = new File(rainLocation, "scadr.jar")

    if(rainJar.exists && scadrJar.exists)
      S3CachedJar(S3Cache.getCacheUrl(rainJar.getCanonicalPath)) ::
      S3CachedJar(S3Cache.getCacheUrl(scadrJar.getCanonicalPath)) :: Nil
    else
      S3CachedJar("http://s3.amazonaws.com/deploylibCache-rean/f2f74da753d224836fedfd56c496c50a") ::
      S3CachedJar("http://s3.amazonaws.com/deploylibCache-rean/3971dfa23416db1b74d47af9b9d3301d") :: Nil
  }

  implicit def classSource = MesosEC2.classSource

  def toHtml: scala.xml.NodeSeq = <div>RADLab Demo Setup: <a href={"http://" + serviceScheduler.host + ":8080"}>Mesos Master</a></div>
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
case class WebAppSchedulerTask(var name: String, var mesosMaster: String, var executor: String, var warFile: S3CachedJar, var zkWebServerListRoot: String, var properties: Map[String, String]) extends AvroTask with AvroRecord {
  import DemoConfig._

  def run(): Unit = {
    System.loadLibrary("mesos")
    val scheduler = new WebAppScheduler(name, mesosMaster, executor, warFile, properties, zkWebServerListRoot, 1, Some(dashboardDb))
    scheduler.monitorThread.join()
  }
}

object IntKeyScaleScheduler extends optional.Application {
  import DemoConfig._

  def main(name: String, mesosMaster: String, executor: String, cp: String): Unit = {
    System.loadLibrary("mesos")
    implicit val scheduler = LocalExperimentScheduler(name, mesosMaster, executor)
    implicit val classpath = cp.split("\\|").map(S3CachedJar(_)).toSeq
    implicit val zookeeper = zooKeeperRoot.getOrCreate("scads/perf") 

    import scads.perf.intkey._
    val cluster = DataLoader(1,1).newCluster
    RandomLoadClient(1, 1).schedule(cluster)
  }
}

object RepTestScheduler extends optional.Application {
  import DemoConfig._

  def main(name: String, mesosMaster: String, executor: String, cp: String, numKeys: Int): Unit = {
    System.loadLibrary("mesos")
    implicit val scheduler = LocalExperimentScheduler(name, mesosMaster, executor)
    implicit val classpath = cp.split("\\|").map(S3CachedJar(_)).toSeq
    implicit val zookeeper = zooKeeperRoot.getOrCreate("scads/perf") 

    import scads.perf.reptest._
    val cluster = DataLoader(1, numKeys).newCluster
    RepClient(3).schedule(cluster)

    cluster.root.awaitChild("expFinished")

    println("--- Printing results for exp with %d keys ---".format(numKeys))
    val loadResults = cluster.getNamespace[RepResultKey, RepResultValue]("repResults")
    println("iteration\telasped time (ms)")
    loadResults.getRange(None, None).foreach { case (k, v) => 
      println("%d\t%d".format(k.iteration, v.endTime - v.startTime))
    }

    //System.exit(0)
  }
}
