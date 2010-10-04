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

trait ExperimentPart extends optional.Application {
  implicit val zooRoot = ZooKeeperNode(Config.config("mesos.zooKeeperRoot", "zk://r2.millennium.berkeley.edu:2181/"))
  val logger = Logger()
  val resultCluster = new ScadsCluster(ZooKeeperNode("zk://r2.millennium.berkeley.edu:2181/scads/results"))
}

trait Experiment extends ExperimentPart {
  val name = this.getClass.getName
  val mesosMaster = Config.config("mesos.master", "1@" + java.net.InetAddress.getLocalHost.getHostName + ":5050")
  val scheduler = ServiceScheduler(name, mesosMaster)
  val expRoot = zooRoot.getOrCreate("scads/experiments").createChild("IntKeyScaleExperiment", mode = CreateMode.PERSISTENT_SEQUENTIAL)

  def getExperimentalCluster(clusterSize: Int): ScadsMesosCluster = {
    val cluster = new ScadsMesosCluster(expRoot, scheduler, clusterSize)
    println("Cluster located at: " + cluster.root)
    cluster.blockTillReady
    cluster
  }
}
