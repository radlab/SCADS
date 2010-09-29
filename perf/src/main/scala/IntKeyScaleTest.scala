package edu.berkeley.cs.scads.perf

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
case class ReadPerfResult(var numKeys: Int, var startTime: Long, var endTime: Long) extends AvroRecord

object IntKeyScaleTest extends Experiment {
  def main(clusterSize: Int, recsPerServer: Int = 10000, readCount: Int = 1000, readThreads: Int = 10, numIterations: Int = 5): Unit = {
    println("Begining cluster scale test with " + clusterSize + " servers. With Scheduler %s", scheduler)

    val cluster = getExperimentalCluster(clusterSize)

    val keySplits = None +: ((clusterSize + 1) to (clusterSize - 1)).map(i => Some(IntRec(i * recsPerServer))) :+  None
    cluster.createNamespace[IntRec, IntRec]("intkeytest", keySplits zip cluster.getAvailableServers.map(List(_)))
    val writeResults = cluster.getNamespace[WriteClient, WritePerfResult]("writeResults")
    val readResults = cluster.getNamespace[ReadClient, ReadPerfResult]("readResults")

    val jars = new File(baseDir, "perf-2.1.0-SNAPSHOT.jar") :: new File(baseDir, "mesos-scads-2.1.0-SNAPSHOT-jar-with-dependencies.jar") :: Nil
    val procDesc = JvmProcess(
      jars.map(_.toString).mkString(":"),
      "edu.berkeley.cs.scads.perf.IntKeyScaleClient",
      "--clusterSize" :: clusterSize.toString ::
      "--recsPerServer" :: recsPerServer.toString ::
      "--clusterAddress" :: cluster.root.canonicalAddress ::
      "--readCount" :: readCount.toString ::
      "--numIterations" :: numIterations.toString ::
      "--readThreads" :: readThreads.toString:: Nil)

    val coordination = cluster.root.getOrCreate("coordination")
    (1 to clusterSize).foreach(i => scheduler.runService(512, 1, procDesc))

    println("Waiting for clients to start")
    coordination.getOrCreate("startWrite").awaitChild("client", clusterSize - 1)

    for(i <- numIterations) {
      println("Waiting for writing to complete")
      coordination.getOrCreate("endWrite" + i).awaitChild("client", clusterSize - 1)
      println("Begining Read Portion of Test")
      coordination.getOrCreate("endRead" + i).awaitChild("client", clusterSize - 1)
      println("Deleting keys")
      coordination.getOrCreate("endDelete" + i).awaitChild("client", clusterSize - 1)
    }

    println("Test Done")

    writeResults.getRange(None, None).map(r => "Client: %d, %f".format(r._1.clientId, r._2.numKeys.toFloat / ((r._2.endTime - r._2.startTime) / 1000))).foreach(println)
    val r = readResults.getRange(None, None)
    val readStart = r.map(_._2.startTime).min
    val readEnd = r.map(_._2.endTime).max
    val readsTotal = r.map(_._2.numKeys).sum
    println("Total Reads: " + readsTotal)
    println("Read Performance: " + readsTotal.toFloat / ((readEnd - readStart) / 1000))

    System.exit(0)
  }
}

object IntKeyScaleClient extends ExperimentPart {
  def main(clusterSize: Int, numIterations: Int, recsPerServer: Int, clusterAddress: String, readCount: Int, readThreads: Int): Unit = {
    val clusterRoot = ZooKeeperNode(clusterAddress)
    val coordination = clusterRoot("coordination")
    val cluster = new ScadsCluster(clusterRoot)
    val ns = cluster.getNamespace[IntRec, IntRec]("intkeytest")
    val writeResults = cluster.getNamespace[WriteClient, WritePerfResult]("writeResults")
    val readResults = cluster.getNamespace[ReadClient, ReadPerfResult]("readResults")

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
        val startTime = System.currentTimeMillis
        var successes = 0
        (1 to readCount).foreach(i =>
          try {
            val randRec = scala.util.Random.nextInt(clusterSize * recsPerServer)
            if(ns.get(IntRec(randRec)).get.f1 == randRec)
              successes += 1
            else
              logger.warning("Failed Read")
          }
          catch { case e =>  logger.debug("Exception during read %s", e)}
        )
        (ReadClient(clusterAddress, clientId, threadId, iteration), ReadPerfResult(successes, startTime, System.currentTimeMillis))
      })

      coordination.registerAndAwait("endRead" + iteration, clusterSize)
      if(clientId == 0) {
        logger.info("Begining delete of IntKey namespace")
        ns.delete
        logger.info("Delete complete")
      }
      coordination.registerAndAwait("endDelete" + iteration, clusterSize)
    }

    System.exit(0)
  }
}
