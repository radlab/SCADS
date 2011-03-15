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

case class DataLoader(var numLoaders: Int, var recsPerLoader: Int, var numServers: Int = 2) extends DataLoadingTask with AvroRecord with StorageServiceSorter {

  var clusterAddress: String = _

  def run(): Unit = {
    require(numLoaders >= 1, "need >= 1 data loader")
    require(recsPerLoader > 0, "need > 0 recs per loader")
    require(numServers >= 1, "need >= 1 server")
    
    val clusterRoot = ZooKeeperNode(clusterAddress)
    val coordination = clusterRoot.getOrCreate("coordination/loaders")
    val cluster = new ExperimentalScadsCluster(clusterRoot)

    val clientId = coordination.registerAndAwait("clientsStart", numLoaders)
    if (clientId == 0) {
      cluster.blockUntilReady(numServers)
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

case class RepClient(var numIterations: Int, var clusterAddress: String) extends ReplicatedExperimentTask with AvroRecord with StorageServiceSorter {

  /** For now, only a single rep client runs and issues replication calls */ 
  var numClients = 1
  var experimentAddress: String = _
  var resultClusterAddress: String = _

  protected lazy val test = 1

  def run(): Unit = {
    val clusterRoot = ZooKeeperNode(clusterAddress)
    val experimentRoot = ZooKeeperNode(experimentAddress)
    val dataLoader = classOf[DataLoader].newInstance.parse(new String(clusterRoot.awaitChild("clusterReady").data))

    val cluster = new ScadsCluster(clusterRoot)
    val ns = cluster.getNamespace[IntRec, IntRec]("repscaletest")
    val loadResults = cluster.getNamespace[RepResultKey, RepResultValue]("repResults")
    val servers = orderStorageServices(cluster.getAvailableServers).toIndexedSeq

    for (iteration <- (1 to numIterations)) {
      logger.info("Begining Iteration %d", iteration)

      // next storage service to replicate to
      val storageService = servers(numIterations % servers.size)

      val oldPartitions = ns.routingTable.ranges.flatMap(_.values).toIndexedSeq

      val startTime = System.currentTimeMillis
      // replicate all partitions to the next storage service
      ns.replicatePartitions(oldPartitions.zip(List.fill(oldPartitions.size)(storageService)))
      val endTime = System.currentTimeMillis

      loadResults.put(RepResultKey(clusterRoot.canonicalAddress, iteration), Some(RepResultValue(startTime, endTime)))

      // delete the existing partitions which were just replicated onto
      // storageService
      ns.deletePartitions(oldPartitions)
    }

    clusterRoot.createChild("expFinished")
  }
}

