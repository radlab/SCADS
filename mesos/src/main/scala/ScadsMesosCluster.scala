package edu.berkeley.cs.scads.mesos

import mesos._
import java.io.File
import net.lag.logging.Logger

import org.apache.zookeeper.CreateMode

import edu.berkeley.cs.scads.comm._
import edu.berkeley.cs.scads.storage.ScadsCluster

import scala.collection.JavaConversions._

class ScadsMesosCluster(root: ZooKeeperProxy#ZooKeeperNode, val scheduler: ServiceScheduler, initialSize: Int, cores: Int = 2, mem: Int = 5000) extends ScadsCluster(root) {
  val logger = Logger()
  val configLocation = new File(scheduler.basePath, "scads.config")
  val jarLocation = new File(scheduler.basePath, "mesos-scads-2.1.0-SNAPSHOT-jar-with-dependencies.jar")
  val procDesc = JvmProcess(jarLocation.toString ,
    "edu.berkeley.cs.scads.storage.ScalaEngine",
    "--zooKeeper" :: root.proxy.address :: "--zooBase" :: root.path :: "--verbose" :: Nil,
    Map("scads.config" -> configLocation.toString))

  (1 to initialSize).foreach(i => scheduler.runService(mem, cores, procDesc))

  def blockTillReady: Unit = {
    while(getAvailableServers.size < initialSize) {
      logger.info("Waiting for cluster to start " + getAvailableServers.size + " of " + initialSize + " ready.")
      Thread.sleep(1000)
    }
  }
}
