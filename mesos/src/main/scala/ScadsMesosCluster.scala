package edu.berkeley.cs.scads.mesos

import mesos._
import deploylib.mesos._

import java.io.File
import net.lag.logging.Logger

import edu.berkeley.cs.scads.comm._
import edu.berkeley.cs.scads.storage.ScadsCluster

import scala.collection.JavaConversions._

class ScadsMesosCluster(root: ZooKeeperProxy#ZooKeeperNode, val scheduler: ServiceScheduler, initialSize: Int, cores: Int = 2, mem: Int = 5000) extends ScadsCluster(root) {
  val logger = Logger()
  val jarLocation = "/root/mesos-scads-2.1.0-SNAPSHOT-jar-with-dependencies.jar"
  val procDesc = JvmProcess(ServerSideJar(jarLocation) :: Nil ,
    "edu.berkeley.cs.scads.storage.ScalaEngine",
    "--zooKeeper" :: root.proxy.address :: "--zooBase" :: root.path :: "--verbose" :: Nil)

  (1 to initialSize).foreach(i => scheduler.runService(mem, cores, procDesc))

  def blockTillReady: Unit = {
    while(getAvailableServers.size < initialSize) {
      logger.info("Waiting for cluster to start " + getAvailableServers.size + " of " + initialSize + " ready.")
      Thread.sleep(1000)
    }
  }
}
