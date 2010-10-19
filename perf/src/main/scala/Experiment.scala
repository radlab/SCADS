package edu.berkeley.cs.scads.perf

import deploylib.ec2._
import deploylib.mesos._
import edu.berkeley.cs.scads.comm._
import edu.berkeley.cs.scads.config._
import edu.berkeley.cs.scads.storage._
import edu.berkeley.cs.avro.runtime._
import edu.berkeley.cs.avro.marker._

import org.apache.avro.generic.IndexedRecord
import org.apache.zookeeper.CreateMode

import java.io.File
import net.lag.logging.Logger

abstract trait AvroClient extends IndexedRecord {
  val logger = Logger()

  def run(clusterRoot: ZooKeeperProxy#ZooKeeperNode)

  implicit def blockingCluster(cluster: ScadsCluster) = new {
    def blockUntilReady(clusterSize: Int): Unit = {
      while(cluster.getAvailableServers.size < clusterSize) {
        logger.info("Waiting for cluster to start " + cluster.getAvailableServers.size + " of " + clusterSize + " ready.")
        Thread.sleep(1000)
      }
    }
  }
}

object AvroClientMain {
  val logger = Logger()

  def main(args: Array[String]): Unit = {
    if(args.size == 3)
    try {
      val clusterRoot = ZooKeeperNode(args(1))
      Class.forName(args(0)).newInstance.asInstanceOf[AvroClient].parse(args(2)).run(clusterRoot)
    }
    catch {
      case error => {
        logger.fatal(error, "Exeception in Main Thread.  Killing process.")
        System.exit(-1)
      }
    }
    else
      println("Usage: " + this.getClass.getName + "<class name> <zookeeper address> <json encoded avro client description>")
  }
}


abstract trait Experiment {
  val logger = Logger()
  lazy implicit val zookeeper = new ZooKeeperProxy("mesos-ec2.knowsql.org:2181")
  lazy val resultCluster = new ScadsCluster(zookeeper.root.getOrCreate("scads/results"))

  implicit def duplicate(process: JvmProcess) = new {
    def *(count: Int): Seq[JvmProcess] = Array.fill(count)(process)
  }

  def newExperimentRoot = zookeeper.root.getOrCreate("scads/experiments").createChild("IntKeyScaleExperiment", mode = CreateMode.PERSISTENT_SEQUENTIAL)

  def serverJvmProcess(clusterAddress: String)(implicit classpath: Seq[ClassSource]) =
    JvmProcess(
      classpath,
      "edu.berkeley.cs.scads.storage.ScalaEngine",
      "--clusterAddress" :: clusterAddress :: Nil)

  implicit def clientJvmProcess(loadClient: AvroClient, clusterRoot: ZooKeeperProxy#ZooKeeperNode)(implicit classpath: Seq[ClassSource]): JvmProcess =
    JvmProcess(classpath,
      "edu.berkeley.cs.scads.perf.AvroClientMain",
      loadClient.getClass.getName :: clusterRoot.canonicalAddress :: loadClient.toJson :: Nil)
}
