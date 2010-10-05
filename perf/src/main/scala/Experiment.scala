package edu.berkeley.cs.scads.perf

import deploylib.mesos._
import edu.berkeley.cs.scads.comm._
import edu.berkeley.cs.scads.mesos._
import edu.berkeley.cs.scads.config._
import edu.berkeley.cs.scads.storage._
import edu.berkeley.cs.avro.runtime._
import edu.berkeley.cs.avro.marker._
import org.apache.zookeeper.CreateMode

import net.lag.logging.Logger

trait Experiment extends optional.Application {
  implicit val zooRoot = ZooKeeperNode(Config.config("mesos.zooKeeperRoot", "zk://r2.millennium.berkeley.edu:2181/"))
  val logger = Logger()
  val resultCluster = new ScadsCluster(ZooKeeperNode("zk://r2.millennium.berkeley.edu:2181/scads/results"))

  val jarPath = ServerSideJar("/root/perf-2.1.0-SNAPSHOT.jar") ::
                ServerSideJar("/root/perf-2.1.0-SNAPSHOT-jar-with-dependencies.jar") :: Nil

  lazy val scheduler = startScheduler
  private def startScheduler: ExperimentScheduler = {
    val mesosMaster = Config.config("mesos.master", "1@" + java.net.InetAddress.getLocalHost.getHostName + ":5050")
    ExperimentScheduler("Scads Perf Experiment: " + this.getClass.getName, mesosMaster)
  }

  implicit def blockingCluster(cluster: ScadsCluster) = new {
    def blockUntilReady(clusterSize: Int): Unit = {
      while(cluster.getAvailableServers.size < clusterSize) {
        logger.info("Waiting for cluster to start " + cluster.getAvailableServers.size + " of " + clusterSize + " ready.")
        Thread.sleep(1000)
      }
    }
  }

}
