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
  def blockUntilReady(clusterSize: Int): Unit = {
    while(getAvailableServers.size < clusterSize) {
      logger.info("Waiting for cluster to start " + cluster.getAvailableServers.size + " of " + clusterSize + " ready.")
      Thread.sleep(1000)
    }
  }
}

trait ExperimentBase {
  var resultClusterBase = Config.config.getString("scads.perf.resultZooKeeperAddress").getOrElse(sys.error("need to specify scads.perf.resultZooKeeperAddress"))
  var resultClusterAddress = resultClusterBase + "home/" + System.getenv("USER") + "/deploylib/"
  val resultCluster = new ScadsCluster(ZooKeeperNode(resultClusterAddress))

  def relativeAddress(suffix: String): String = {
    resultClusterBase + "home/" + System.getenv("USER") + "/" + suffix
  }

  implicit def productSeqToExcel(lines: Seq[Product]) = new {
    import java.io._
    def toExcel: Unit = {
      val file = File.createTempFile("scadsOut", ".csv")
      val writer = new FileWriter(file)

      lines.map(_.productIterator.mkString(",") + "\n").foreach(writer.write)
      writer.close

      Runtime.getRuntime.exec(Array("/usr/bin/open", file.getCanonicalPath))
    }
  }
}

trait TaskBase {
  def newScadsCluster(size: Int)(implicit cluster: Cluster, classSource: Seq[ClassSource]): ScadsCluster = {
    val clusterRoot = cluster.zooKeeperRoot.getOrCreate("scads").createChild("experimentCluster", mode = CreateMode.PERSISTENT_SEQUENTIAL)
    val serverProcs = Array.fill(size)(ScalaEngineTask(clusterAddress=clusterRoot.canonicalAddress).toJvmTask)
    serverProcs.foreach(_.foreach(_.props += "scads.mdcc.fastDefault" -> "false"))
    serverProcs.foreach(_.foreach(_.props += "scads.mdcc.DefaultRounds" -> "999999999999"))

    cluster.serviceScheduler.scheduleExperiment(serverProcs)
    new ScadsCluster(clusterRoot)
  }
}
