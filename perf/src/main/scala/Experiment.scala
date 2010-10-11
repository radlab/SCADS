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

  implicit def blockingCluster(cluster: ScadsCluster) = new {
    def blockUntilReady(clusterSize: Int): Unit = {
      while(cluster.getAvailableServers.size < clusterSize) {
        logger.info("Waiting for cluster to start " + cluster.getAvailableServers.size + " of " + clusterSize + " ready.")
        Thread.sleep(1000)
      }
    }
  }

  def run(clients: List[IndexedRecord], numServers: Int)(implicit classpath: Seq[ClassSource], scheduler: ExperimentScheduler): Unit = {
    val expRoot = zooRoot.getOrCreate("scads/experiments").createChild("IntKeyScaleExperiment", mode = CreateMode.PERSISTENT_SEQUENTIAL)
    clients.foreach(c => c.put(c.getSchema.getField("clusterAddress").pos, new org.apache.avro.util.Utf8(expRoot.canonicalAddress)))
    val clientDescriptions = clients.map(c => JvmProcess(classpath, this.getClass.getCanonicalName.dropRight(1), c.getClass.getName :: c.toJson :: Nil))
    val serverDescription = JvmProcess(
      classpath,
      "edu.berkeley.cs.scads.storage.ScalaEngine",
      "--clusterAddress" :: expRoot.canonicalAddress :: Nil)

    scheduler.scheduleExperiment((1 to numServers).map(_ => serverDescription) ++ clientDescriptions)
    expRoot
  }

  def main(args: Array[String]): Unit = {
    if(args.size == 2)
      Class.forName(args(0)).newInstance.asInstanceOf[AvroRecord with Runnable].parse(args(1)).run()
    else
      println("Usage: " + this.getClass.getName + "<class name> <json encoded avro client description>")
  }
}
