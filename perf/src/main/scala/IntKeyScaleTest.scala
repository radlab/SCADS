package edu.berkeley.cs.scads.perf

import deploylib.mesos._
import edu.berkeley.cs.scads.comm._
import edu.berkeley.cs.scads.mesos._
import edu.berkeley.cs.scads.config._
import edu.berkeley.cs.scads.storage._
import edu.berkeley.cs.avro.runtime._
import edu.berkeley.cs.avro.marker._

import org.apache.zookeeper.CreateMode


import ParallelConversions._

import java.io.File

case class WriteClient(var cluster: String, var clientId: Int, var iteration: Int) extends AvroRecord
case class WritePerfResult(var numKeys: Int, var startTime: Long, var endTime: Long) extends AvroRecord

case class ReadClient(var cluster: String, var clientId: Int, var threadId: Int, var iteration: Int) extends AvroRecord
case class ReadPerfResult(var startTime: Long, var endTime: Long, var times: Histogram) extends AvroRecord

object IntKeyScaleTest extends Experiment {
  def main(clusterSize: Int, recsPerServer: Int = 10000, readCount: Int = 1000, readThreads: Int = 10, numIterations: Int = 5): Unit = {
    println("Begining cluster scale test with " + clusterSize + " servers. With Scheduler %s", scheduler)

    val writeResults = resultCluster.getNamespace[WriteClient, WritePerfResult]("writeResults")
    val readResults = resultCluster.getNamespace[ReadClient, ReadPerfResult]("readResults")

    val cluster = getExperimentalCluster(clusterSize)
    val keySplits = None +: ((clusterSize + 1) to (clusterSize - 1)).map(i => Some(IntRec(i * recsPerServer))) :+  None
    cluster.createNamespace[IntRec, IntRec]("intkeytest", keySplits zip cluster.getAvailableServers.map(List(_)))

    val jarPath = ServerSideJar("/root/perf-2.1.0-SNAPSHOT-jar-with-dependencies.jar") :: Nil
    val procDesc = JvmProcess(
      jarPath,
      "edu.berkeley.cs.scads.perf.IntKeyScaleClient",
      "--clusterSize" :: clusterSize.toString ::
      "--recsPerServer" :: recsPerServer.toString ::
      "--clusterAddress" :: cluster.root.canonicalAddress ::
      "--readCount" :: readCount.toString ::
      "--numIterations" :: numIterations.toString ::
      "--readThreads" :: readThreads.toString:: Nil)

    val coordination = cluster.root.getOrCreate("coordination")
    (1 to clusterSize).foreach(i => scheduler.runService(4000, 2, procDesc))

    println("Waiting for clients to start")
    coordination.getOrCreate("startWrite").awaitChild("client", clusterSize - 1)

    for(i <- 1 to numIterations) {
      println("Waiting for writing to complete")
      coordination.getOrCreate("endWrite" + i).awaitChild("client", clusterSize - 1)
      println("Begining Read Portion of Test")
      val readStartTime = System.currentTimeMillis
      coordination.getOrCreate("endRead" + i).awaitChild("client", clusterSize - 1)
      val readEndTime = System.currentTimeMillis
      println("Reads Completed in: %d seconds".format((readEndTime - readStartTime) / 1000))
    }

    println("Test Done")

    writeResults.getRange(None, None).map(r => "Client: %d, %f".format(r._1.clientId, r._2.numKeys.toFloat / ((r._2.endTime - r._2.startTime) / 1000))).foreach(println)
    readResults.getRange(None, None).foreach(println)

    System.exit(0)
  }
}

object IntKeyScaleClient extends ExperimentPart {
  def main(clusterSize: Int, numIterations: Int, recsPerServer: Int, clusterAddress: String, readCount: Int, readThreads: Int): Unit = {
    val clusterRoot = ZooKeeperNode(clusterAddress)
    val coordination = clusterRoot("coordination")
    val cluster = new ScadsCluster(clusterRoot)
    val ns = cluster.getNamespace[IntRec, IntRec]("intkeytest")

    val writeResults = resultCluster.getNamespace[WriteClient, WritePerfResult]("writeResults")
    val readResults = resultCluster.getNamespace[ReadClient, ReadPerfResult]("readResults")

    val clientId = coordination.registerAndAwait("startWrite", clusterSize)
    val startKey = clientId * recsPerServer
    val endKey = (clientId + 1) * recsPerServer

    for(iteration <- (1 to numIterations)) {
      logger.info("Begining Iteration %d", iteration)

      val startTime = System.currentTimeMillis
      ns ++= (startKey to endKey).view.map(i => (IntRec(i), IntRec(i)))
      writeResults.put(WriteClient(clusterAddress, clientId, iteration), WritePerfResult(recsPerServer, startTime, System.currentTimeMillis))

      coordination.registerAndAwait("endWrite" + iteration, clusterSize)

      readResults ++= (1 to readThreads).pmap(threadId => {
        val times = Histogram(10, 50)
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

    System.exit(0)
  }
}
