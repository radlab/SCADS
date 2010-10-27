package edu.berkeley.cs.scads.perf

import deploylib.ec2._
import deploylib.mesos._
import edu.berkeley.cs.scads.comm._
import edu.berkeley.cs.scads.config._
import edu.berkeley.cs.scads.storage._
import edu.berkeley.cs.avro.runtime._
import edu.berkeley.cs.avro.marker._

import org.apache.zookeeper.CreateMode

import java.io.File
import net.lag.logging.Logger

class ExperimentalScadsCluster(root: ZooKeeperProxy#ZooKeeperNode) extends ScadsCluster(root) {
  val logger = Logger()

  def blockUntilReady(clusterSize: Int): Unit = {
    while(getAvailableServers.size < clusterSize) {
      logger.info("Waiting for cluster to start " + cluster.getAvailableServers.size + " of " + clusterSize + " ready.")
      Thread.sleep(1000)
    }
  }
}

abstract trait Experiment {
  val logger = Logger()
  lazy val zookeeper = new ZooKeeperProxy("mesos-ec2.knowsql.org:2181")
  lazy val resultCluster = new ScadsCluster(zookeeper.root.getOrCreate("scads/results"))

  implicit def duplicate(process: JvmProcess) = new {
    def *(count: Int): Seq[JvmProcess] = Array.fill(count)(process)
  }

  def newExperimentRoot(implicit zookeeper: ZooKeeperProxy#ZooKeeperNode)  = zookeeper.getOrCreate("scads/experiments").createChild("IntKeyScaleExperiment", mode = CreateMode.PERSISTENT_SEQUENTIAL)

  def serverJvmProcess(clusterAddress: String)(implicit classpath: Seq[ClassSource]) =
    JvmProcess(
      classpath,
      "edu.berkeley.cs.scads.storage.ScalaEngine",
      "--clusterAddress" :: clusterAddress :: Nil)

  def clientJvmProcess(loadClient: AvroClient, clusterRoot: ZooKeeperProxy#ZooKeeperNode)(implicit classpath: Seq[ClassSource]): JvmProcess =
    JvmProcess(classpath,
      "edu.berkeley.cs.scads.perf.AvroClientMain",
      loadClient.getClass.getName :: clusterRoot.canonicalAddress :: loadClient.toJson :: Nil)

  def run(clientDesc: ReplicatedAvroClient, cluster: ScadsCluster)(implicit classpath: Seq[ClassSource], scheduler: ExperimentScheduler): Unit = {
    cluster.root("coordination").get("clients").foreach(_.deleteRecursive)
    scheduler.scheduleExperiment(clientJvmProcess(clientDesc, cluster.root) * clientDesc.numClients)
  }
}
