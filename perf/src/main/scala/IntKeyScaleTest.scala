package edu.berkeley.cs.scads.perf
package intkey

import deploylib._
import deploylib.mesos._
import edu.berkeley.cs.scads.comm._
import edu.berkeley.cs.scads.config._
import edu.berkeley.cs.scads.storage._
import edu.berkeley.cs.avro.runtime._
import edu.berkeley.cs.avro.marker._

import org.apache.zookeeper.CreateMode

import java.io.File 

object IntKeyScaleTest extends ExperimentMain {
  def main(args: Array[String]): Unit = {
    val cluster = DataLoader(1,1).newCluster
    RandomLoadClient(1, 1).schedule(cluster)
  }
}

case class WriteClient(var cluster: String, var clientId: Int) extends AvroRecord
case class WritePerfResult(var numKeys: Int, var startTime: Long, var endTime: Long) extends AvroRecord

case class LoadClient(var cluster: String, var clientId: Int, var threadId: Int, var iteration: Int) extends AvroRecord
case class LoadPerfResult(var startTime: Long, var endTime: Long, var getTimes: Histogram, var putTimes: Histogram) extends AvroRecord

case class DataLoader(var numServers: Int, var numLoaders: Int, var recsPerServer: Int = 10) extends DataLoadingAvroClient with AvroRecord {
  def run(clusterRoot: ZooKeeperProxy#ZooKeeperNode): Unit = {
    val coordination = clusterRoot.getOrCreate("coordination/loaders")
    val cluster = new ExperimentalScadsCluster(clusterRoot)

    val clientId = coordination.registerAndAwait("clientsStart", numServers)
    if (clientId == 0) {
      cluster.blockUntilReady(numServers)

      val keySplits = None +: (1 until numServers).map(i => Some(IntRec(i * recsPerServer)))
      val partitions = keySplits zip cluster.getAvailableServers.map(List(_))
      logger.info("Cluster configured with the following partitions %s", partitions)
      cluster.createNamespace[IntRec, IntRec]("intkeytest", partitions)
      cluster.getNamespace[WriteClient, WritePerfResult]("writeResults")
      cluster.getNamespace[LoadClient, LoadPerfResult]("loadResults")
    }

    coordination.registerAndAwait("startWrite", numLoaders)
    val writeResults = cluster.getNamespace[WriteClient, WritePerfResult]("writeResults")
    val ns = cluster.getNamespace[IntRec, IntRec]("intkeytest")
    val startKey = clientId * recsPerServer
    val endKey = (clientId + 1) * recsPerServer

    val startTime = System.currentTimeMillis
    logger.info("Starting bulk put")
    ns ++= (startKey to endKey).view.map(i => (IntRec(i), IntRec(i)))
    logger.info("Bulk put complete")
    writeResults.put(WriteClient(clusterRoot.canonicalAddress, clientId), WritePerfResult(recsPerServer, startTime, System.currentTimeMillis))
    coordination.registerAndAwait("endWrite", numLoaders)

    if (clientId == 0)
      clusterRoot.createChild("clusterReady", data = this.toJson.getBytes)
  }
}

case class RandomLoadClient(var numClients: Int, var numIterations: Int, var loadCount: Int = 10000, var loadThreads: Int = 5, var readPercentage: Double = 0.8) extends ReplicatedAvroClient with AvroRecord {

  def run(clusterRoot: ZooKeeperProxy#ZooKeeperNode): Unit = {
    require(0.0 <= readPercentage && readPercentage <= 1.0, "Read percentage needs to be between [0.0, 1.0]")

    val coordination = clusterRoot.getOrCreate("coordination/clients")
    val dataLoader = classOf[DataLoader].newInstance.parse(new String(clusterRoot.awaitChild("clusterReady").data))
    val maxInt = dataLoader.numServers * dataLoader.recsPerServer

    val cluster = new ScadsCluster(clusterRoot)
    val ns = cluster.getNamespace[IntRec, IntRec]("intkeytest")
    val loadResults = cluster.getNamespace[LoadClient, LoadPerfResult]("loadResults")

    val clientId = coordination.registerAndAwait("clientsStart", numClients)

    for (iteration <- (1 to numIterations)) {
      logger.info("Begining Iteration %d", iteration)

      loadResults ++= (1 to loadThreads).pmap(threadId => {
        val getTimes = Histogram(1, 1000)
        val putTimes = Histogram(1, 1000)
        val startTime = System.currentTimeMillis()
        (1 to loadCount).foreach(i =>
          try {
            val startTime = System.currentTimeMillis
            val randRec = scala.util.Random.nextInt(maxInt)
            val isRead = scala.util.Random.nextDouble() < readPercentage

            if (isRead) {
              if (ns.get(IntRec(randRec)).get.f1 == randRec) {
                val endTime = System.currentTimeMillis
                getTimes.add(endTime - startTime)
              } else
                logger.warning("Failed Read")
            } else {
              ns.put(IntRec(randRec), IntRec(randRec))
              val endTime = System.currentTimeMillis
              putTimes.add(endTime - startTime)
            }
          } catch { case e => logger.debug("Exception during operation %s", e) })
        (LoadClient(clusterRoot.canonicalAddress, clientId, threadId, iteration), LoadPerfResult(startTime, System.currentTimeMillis, getTimes, putTimes))
      })

      coordination.registerAndAwait("endRead" + iteration, numClients)
    }
  }
}
