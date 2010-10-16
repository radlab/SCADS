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

abstract trait Experiment {
  implicit val zooRoot = ZooKeeperNode(Config.config("mesos.zooKeeperRoot", "zk://r2.millennium.berkeley.edu:2181/"))
  val logger = Logger()
  val resultCluster = new ScadsCluster(ZooKeeperNode("zk://r2.millennium.berkeley.edu:2181/scads/results"))

  abstract class Client extends Runnable with IndexedRecord {
    var clusterAddress: String
  }

  implicit def duplicate(process: JvmProcess) = new {
    def *(count: Int): Seq[JvmProcess] = Array.fill(count)(process)
  }

  implicit def blockingCluster(cluster: ScadsCluster) = new {
    def blockUntilReady(clusterSize: Int): Unit = {
      while(cluster.getAvailableServers.size < clusterSize) {
        logger.info("Waiting for cluster to start " + cluster.getAvailableServers.size + " of " + clusterSize + " ready.")
        Thread.sleep(1000)
      }
    }
  }

  def newExperimentRoot = zooRoot.getOrCreate("scads/experiments").createChild("IntKeyScaleExperiment", mode = CreateMode.PERSISTENT_SEQUENTIAL)

  def serverJvmProcess(clusterAddress: String)(implicit classpath: Seq[ClassSource]) =
    JvmProcess(
      classpath,
      "edu.berkeley.cs.scads.storage.ScalaEngine",
      "--clusterAddress" :: clusterAddress :: Nil)

  implicit def clientJvmProcess(loadClient: AvroRecord with Runnable)(implicit classpath: Seq[ClassSource]): JvmProcess =
    JvmProcess(classpath,
      this.getClass.getCanonicalName.dropRight(1),
      loadClient.getClass.getName :: loadClient.toJson :: Nil)

  def main(args: Array[String]): Unit = {
    if(args.size == 2)
    try
      Class.forName(args(0)).newInstance.asInstanceOf[AvroRecord with Runnable].parse(args(1)).run()
    catch {
      case error => {
        logger.fatal(error, "Exeception in Main Thread.  Killing process.")
        System.exit(-1)
      }
    }
    else
      println("Usage: " + this.getClass.getName + "<class name> <json encoded avro client description>")
  }
}
