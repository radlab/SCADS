package edu.berkeley.cs.scads.perf
package reptest

import deploylib._
import deploylib.mesos._
import edu.berkeley.cs.scads.comm._
import edu.berkeley.cs.scads.config._
import edu.berkeley.cs.scads.storage._
import edu.berkeley.cs.avro.runtime._
import edu.berkeley.cs.avro.marker._

import org.apache.zookeeper.CreateMode

import java.io.File 

object RepTest extends ExperimentMain {
  def main(args: Array[String]): Unit = {
    val cluster = DataLoader(1, 1000).newCluster
    RepClient(3).schedule(cluster)
  }
}

case class RepResultKey(var cluster: String, var iteration: Int) extends AvroRecord
case class RepResultValue(var startTime: Long, var endTime: Long) extends AvroRecord

trait StorageServiceSorter {
  def orderStorageServices(x: Seq[StorageService]): Seq[StorageService] = {
    x.sortBy(s => (s.host, s.port, s.id match {
      case ActorNumber(x) => x.toString
      case ActorName(x) => x
    }))
  }
}

case class DataLoader(var numLoaders: Int, var recsPerLoader: Int, var numServers: Int = 2) extends DataLoadingAvroClient with AvroRecord with StorageServiceSorter {

  def run(clusterRoot: ZooKeeperProxy#ZooKeeperNode): Unit = {
    require(numLoaders >= 1, "need >= 1 data loader")
    require(recsPerLoader > 0, "need > 0 recs per loader")
    require(numServers >= 1, "need >= 1 server")
    
    val coordination = clusterRoot.getOrCreate("coordination/loaders")
    val cluster = new ExperimentalScadsCluster(clusterRoot)

    val clientId = coordination.registerAndAwait("clientsStart", numServers)
    if (clientId == 0) {
      cluster.blockUntilReady(1)
      cluster.createNamespace[IntRec, IntRec]("repscaletest", List(None -> List(orderStorageServices(cluster.getAvailableServers).head)))
    }

    coordination.registerAndAwait("startWrite", numLoaders)
    val ns = cluster.getNamespace[IntRec, IntRec]("repscaletest")
    val startKey = clientId * recsPerLoader
    val endKey = (clientId + 1) * recsPerLoader

    logger.info("Starting bulk put")
    ns ++= (startKey to endKey).view.map(i => (IntRec(i), IntRec(i)))
    logger.info("Bulk put complete")

    coordination.registerAndAwait("endWrite", numLoaders)

    if (clientId == 0)
      clusterRoot.createChild("clusterReady", data = this.toJson.getBytes)
  }
}

case class RepClient(var numIterations: Int) extends ReplicatedAvroClient with AvroRecord with StorageServiceSorter {

  /** For now, only a single rep client runs and issues replication calls */ 
  var numClients = 1

  def run(clusterRoot: ZooKeeperProxy#ZooKeeperNode): Unit = {

    val dataLoader = classOf[DataLoader].newInstance.parse(new String(clusterRoot.awaitChild("clusterReady").data))

    val cluster = new ScadsCluster(clusterRoot)
    val ns = cluster.getNamespace[IntRec, IntRec]("repscaletest")
    val loadResults = cluster.getNamespace[RepResultKey, RepResultValue]("repResults")

    for (iteration <- (1 to numIterations)) {
      logger.info("Begining Iteration %d", iteration)

      val servers = orderStorageServices(cluster.getAvailableServers).toIndexedSeq
      val storageService = servers(numIterations % servers.size)

      val oldPartitions = ns.partitions.ranges.flatMap(_.values).toIndexedSeq

      val startTime = System.currentTimeMillis
      // replicate all partitions to the next storage service
      ns.replicatePartitions(oldPartitions.zip(List.fill(oldPartitions.size)(storageService)))
      val endTime = System.currentTimeMillis

      loadResults.put(RepResultKey(clusterRoot.canonicalAddress, iteration), Some(RepResultValue(startTime, endTime)))

      // delete the existing partitions which were just replicated onto
      // storageService
      ns.deletePartitions(oldPartitions)
    }
  }
}

