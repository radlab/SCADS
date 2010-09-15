package edu.berkeley.cs.scads.perf

import edu.berkeley.cs.scads.comm._
import edu.berkeley.cs.scads.mesos._
import edu.berkeley.cs.scads.storage._
import com.googlecode.avro.runtime._
import com.googlecode.avro.marker._

import org.apache.zookeeper.CreateMode

import net.lag.logging.Logger

import ParallelConversions._

case class WriteClient(var cluster: String, var clientId: Int) extends AvroRecord
case class WritePerfResult(var numKeys: Int, var startTime: Long, var endTime: Long) extends AvroRecord

case class ReadClient(var cluster: String, var clientId: Int, var threadId: Int) extends AvroRecord
case class ReadPerfResult(var numKeys: Int, var startTime: Long, var endTime: Long) extends AvroRecord

object IntKeyScaleTest extends optional.Application {
  implicit val zooRoot = RClusterZoo.root
  implicit def toOption[A](a: A) = Option(a)
  val logger = Logger()

  def main(clusterSize: Int, recsPerServer: Int = 10000, readCount: Int = 1000): Unit = {
    println("Begining cluster scale test with " + clusterSize + " servers.")
    val cluster = ScadsMesosCluster(clusterSize)
    println("Cluster located at: " + cluster.root)
    cluster.blockTillReady

    val keySplits = None +: ((clusterSize + 1) to (clusterSize - 1)).map(i => Some(IntRec(i * recsPerServer))) :+  None
    cluster.createNamespace[IntRec, IntRec]("intkeytest", keySplits zip cluster.getAvailableServers.map(List(_)))
    val writeResults = cluster.getNamespace[WriteClient, WritePerfResult]("writeResults")
    val readResults = cluster.getNamespace[ReadClient, ReadPerfResult]("readResults")

    val procDesc = JvmProcess(
      "/work/marmbrus/mesos/perf-2.1.0-SNAPSHOT.jar:/work/marmbrus/mesos:/work/marmbrus/mesos/mesos-scads-2.1.0-SNAPSHOT-jar-with-dependencies.jar",
      "edu.berkeley.cs.scads.perf.IntKeyScaleClient",
      "--clusterSize" :: clusterSize.toString :: "--recsPerServer" :: recsPerServer.toString ::"--clusterName" :: cluster.root.name :: "--readCount" :: readCount.toString :: Nil)

    val coordination = cluster.root.getOrCreate("coordination")
    val clients = new ServiceScheduler("IntKey Scale Test")
    (1 to clusterSize).foreach(i => clients.runService(512, 1, procDesc))

    println("Waiting for clients to start")
    coordination.getOrCreate("startWrite").awaitChild("client", clusterSize - 1)
    println("Waiting for writing to complete")
    coordination.getOrCreate("endWrite").awaitChild("client", clusterSize - 1)
    println("Begining Read Portion of Test")
    coordination.getOrCreate("endRead").awaitChild("client", clusterSize - 1)
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

object IntKeyScaleClient extends optional.Application {
  implicit val zooRoot = RClusterZoo.root
  implicit def toOption[A](a: A) = Option(a)
  val logger = Logger()

  def main(clusterSize: Int, recsPerServer: Int, clusterName: String, readCount: Int): Unit = {
    val clusterRoot = zooRoot(clusterName)
    val coordination = clusterRoot("coordination")
    val cluster = new ScadsCluster(clusterRoot)
    val ns = cluster.getNamespace[IntRec, IntRec]("intkeytest")
    val writeResults = cluster.getNamespace[WriteClient, WritePerfResult]("writeResults")
    val readResults = cluster.getNamespace[ReadClient, ReadPerfResult]("readResults")

    def registerAndBlock(name: String):Int  = {
      val node = coordination.getOrCreate(name)
      val seqNum = node.createChild("client", mode = CreateMode.EPHEMERAL_SEQUENTIAL).sequenceNumber
      node.awaitChild("client", clusterSize - 1)
      seqNum
    }

    val clientId = registerAndBlock("startWrite")

    val startKey = clientId * recsPerServer
    val endKey = (clientId + 1) * recsPerServer

    val startTime = System.currentTimeMillis
    ns ++= (startKey to endKey).view.map(i => (IntRec(i), IntRec(i)))
    writeResults.put(WriteClient(clusterName, clientId), WritePerfResult(recsPerServer, startTime, System.currentTimeMillis))

    registerAndBlock("endWrite")

    readResults ++= (1 to 10).pmap(threadId => {
      val startTime = System.currentTimeMillis
      var successes = 0
      (1 to readCount).foreach(i =>
        try {
          ns.get(IntRec(scala.util.Random.nextInt(clusterSize * recsPerServer)))
          successes += 1
        }
        catch { case e =>  logger.debug("Exception during read %s", e)}
      )
      (ReadClient(clusterName, clientId, threadId), ReadPerfResult(successes, startTime, System.currentTimeMillis))
    })

    registerAndBlock("endRead")

    System.exit(0)
  }
}
