package edu.berkeley.cs
package radlab
package demo

import avro.marker._
import avro.runtime._

import scads.comm._
import deploylib.mesos._
import deploylib.ec2._

import net.lag.logging.Logger

import java.io.File

object DemoConfig {
  protected val logger = Logger()
  
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

  val scadrWarFile = new File("piql/scadr/src/main/rails/rails.war")
  def scadrWar =
    if(scadrWarFile.exists)
      S3CachedJar(S3Cache.getCacheUrl(scadrWarFile))
    else {
      logger.info("Using cached scadr war file.")
      S3CachedJar("http://s3.amazonaws.com/deploylibCache-andyk/cf4795cea32f45694ab018ebdf069a39")
    }

  val jdbcDriver = classOf[com.mysql.jdbc.Driver]
  val dashboardDb = "jdbc:mysql://dev-mini-demosql.cwppbyvyquau.us-east-1.rds.amazonaws.com:3306/radlabmetrics?user=radlab_dev&password=randyAndDavelab"

  def rainJars = {
    val rainLocation  = new File("../rain-workload-toolkit")
    val workLoadDir = new File(rainLocation, "workloads")
    val rainJar = new File(rainLocation, "rain.jar")
    val scadrJar = new File(workLoadDir, "scadr.jar")

    if(rainJar.exists && scadrJar.exists)
      S3CachedJar(S3Cache.getCacheUrl(rainJar.getCanonicalPath)) ::
      S3CachedJar(S3Cache.getCacheUrl(scadrJar.getCanonicalPath)) :: Nil
    else
      S3CachedJar("http://s3.amazonaws.com/deploylibCache-rean/f2f74da753d224836fedfd56c496c50a") ::
      S3CachedJar("http://s3.amazonaws.com/deploylibCache-rean/3971dfa23416db1b74d47af9b9d3301d") :: Nil
  }

  implicit def classSource = MesosEC2.classSource

  protected def toServerList(node: ZooKeeperProxy#ZooKeeperNode) = {
    val servers = new String(scadrWebServerList.data).split("\n")
    servers.zipWithIndex.map {
      case (s: String, i: Int) => <a href={"http://%s:8080/".format(s)}>{i}</a>
    }
  }

  def toHtml: scala.xml.NodeSeq = {
    <div>RADLab Demo Setup: <a href={"http://" + serviceScheduler.host + ":8080"}>Mesos Master</a><br/> 
      Scadr Servers: {toServerList(scadrWebServerList)}
    </div>
  }
}
