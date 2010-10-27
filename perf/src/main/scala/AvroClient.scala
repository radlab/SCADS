package edu.berkeley.cs
package scads
package perf

import comm._
import storage._
import avro.runtime._
import avro.marker._
import deploylib.mesos._

import net.lag.logging.Logger
import org.apache.avro.generic.IndexedRecord
import org.apache.zookeeper.CreateMode

abstract trait AvroClient extends IndexedRecord {
  val logger = Logger()

  def run(clusterRoot: ZooKeeperProxy#ZooKeeperNode)

  implicit def duplicate(process: JvmProcess) = new {
    def *(count: Int): Seq[JvmProcess] = Array.fill(count)(process)
  }

  def newExperimentRoot(implicit zookeeper: ZooKeeperProxy#ZooKeeperNode)  = zookeeper.getOrCreate("scads/experiments").createChild("IntKeyScaleExperiment", mode = CreateMode.PERSISTENT_SEQUENTIAL)

  def toJvmProcess(clusterRoot: ZooKeeperProxy#ZooKeeperNode)(implicit classpath: Seq[ClassSource]): JvmProcess =
    JvmProcess(classpath,
      "edu.berkeley.cs.scads.perf.AvroClientMain",
      this.getClass.getName :: clusterRoot.canonicalAddress :: this.toJson :: Nil)

  def serverJvmProcess(clusterAddress: String)(implicit classpath: Seq[ClassSource]) =
    JvmProcess(
      classpath,
      "edu.berkeley.cs.scads.storage.ScalaEngine",
      "--clusterAddress" :: clusterAddress :: Nil)
}

abstract trait ReplicatedAvroClient extends AvroClient {
  var numClients: Int

  def schedule(cluster: ScadsCluster)(implicit classpath: Seq[ClassSource], scheduler: ExperimentScheduler): Unit = {
    cluster.root("coordination").get("clients").foreach(_.deleteRecursive)
    scheduler.scheduleExperiment(toJvmProcess(cluster.root) * numClients)
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
    else {
      println("Usage: " + this.getClass.getName + "<class name> <zookeeper address> <json encoded avro client description>")
      System.exit(-1)
    }
  }
}