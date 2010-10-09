package edu.berkeley.cs.scads.perf

import deploylib._
import deploylib.mesos._
import edu.berkeley.cs.scads.comm._
import edu.berkeley.cs.scads.config._
import edu.berkeley.cs.scads.storage._
import edu.berkeley.cs.avro.runtime._
import edu.berkeley.cs.avro.marker._

import org.apache.zookeeper.CreateMode

import java.io.File

case class WriteClient(var cluster: String, var clientId: Int, var iteration: Int) extends AvroRecord
case class WritePerfResult(var numKeys: Int, var startTime: Long, var endTime: Long) extends AvroRecord

case class ReadClient(var cluster: String, var clientId: Int, var threadId: Int, var iteration: Int) extends AvroRecord
case class ReadPerfResult(var startTime: Long, var endTime: Long, var times: Histogram) extends AvroRecord

object IntKeyScaleTest extends Experiment {
  val writeResults = resultCluster.getNamespace[WriteClient, WritePerfResult]("writeResults")
  val readResults = resultCluster.getNamespace[ReadClient, ReadPerfResult]("readResults")

  case class LoadClient(var clusterSize: Int, var numIterations: Int = 1, var recsPerServer: Int = 10, var readCount: Int = 10000, var readThreads : Int = 5) extends AvroRecord with Runnable {
    var clusterAddress: String = null

    def run(): Unit = {
      val clusterRoot = ZooKeeperNode(clusterAddress)
      val coordination = clusterRoot.getOrCreate("coordination")
      val cluster = new ScadsCluster(clusterRoot)

      val clientId = coordination.registerAndAwait("clientsStart", clusterSize)
      if(clientId == 0) {
        cluster.blockUntilReady(clusterSize)

        val keySplits = None +: (1 to (clusterSize - 1)).map(i => Some(IntRec(i * recsPerServer)))
        val partitions = keySplits zip cluster.getAvailableServers.map(List(_))
        logger.info("Cluster configured with the following partitions %s", partitions)
        cluster.createNamespace[IntRec, IntRec]("intkeytest", partitions)
      }

      coordination.registerAndAwait("startWrite", clusterSize)
      val ns = cluster.getNamespace[IntRec, IntRec]("intkeytest")
      val startKey = clientId * recsPerServer
      val endKey = (clientId + 1) * recsPerServer

      for(iteration <- (1 to numIterations)) {
        logger.info("Begining Iteration %d", iteration)

        val startTime = System.currentTimeMillis
        ns ++= (startKey to endKey).view.map(i => (IntRec(i), IntRec(i)))
        writeResults.put(WriteClient(clusterAddress, clientId, iteration), WritePerfResult(recsPerServer, startTime, System.currentTimeMillis))

        coordination.registerAndAwait("endWrite" + iteration, clusterSize)

        readResults ++= (1 to readThreads).pmap(threadId => {
          val times = Histogram(1, 1000)
          val startTime = System.currentTimeMillis()
          (1 to readCount).foreach(i =>
            try {
              val startTime = System.currentTimeMillis
              val randRec = scala.util.Random.nextInt(clusterSize * recsPerServer)

              if(ns.get(IntRec(randRec)).get.f1 == randRec) {
                val endTime = System.currentTimeMillis
                times.add(endTime - startTime)
              }
              else
                logger.warning("Failed Read")
            }
            catch { case e =>  logger.debug("Exception during read %s", e)}
          )
          (ReadClient(clusterAddress, clientId, threadId, iteration), ReadPerfResult(startTime, System.currentTimeMillis, times))
        })

        coordination.registerAndAwait("endRead" + iteration, clusterSize)
      }

      if(clientId == 0)
        cluster.shutdown

      System.exit(0)
    }
  }
}
