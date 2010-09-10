package edu.berkeley.cs.scads.perf

import edu.berkeley.cs.scads.comm._
import edu.berkeley.cs.scads.mesos._
import edu.berkeley.cs.scads.storage._
import com.googlecode.avro.runtime._
import com.googlecode.avro.marker._

import org.apache.zookeeper.CreateMode

import ParallelConversions._

case class WriteClient(var cluster: String, var clientId: Int) extends AvroRecord
case class WritePerfResult(var numKeys: Int, var startTime: Long, var endTime: Long) extends AvroRecord

case class ReadClient(var cluster: String, var clientId: Int, var threadId: Int) extends AvroRecord
case class ReadPerfResult(var numKeys: Int, var startTime: Long, var endTime: Long) extends AvroRecord

object IntKeyScaleTest extends optional.Application {
  implicit val zooRoot = RClusterZoo.root
  implicit def toOption[A](a: A) = Option(a)

  def main(clusterSize: Int, recsPerServer: Int = 100): Unit = {
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
      "--clusterSize" :: clusterSize.toString :: "--recsPerServer" :: recsPerServer.toString ::"--clusterName" :: cluster.root.name :: Nil)

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
    readResults.getRange(None, None).foreach(println)

    System.exit(0)
  }
}

object IntKeyScaleClient extends optional.Application {
  implicit val zooRoot = RClusterZoo.root
  implicit def toOption[A](a: A) = Option(a)

  def main(clusterSize: Int, recsPerServer: Int, clusterName: String): Unit = {
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
    (startKey to endKey).foreach(i => ns.put(IntRec(i), IntRec(i)))
    writeResults.put(WriteClient(clusterName, clientId), WritePerfResult(recsPerServer, startTime, System.currentTimeMillis))

    registerAndBlock("endWrite")

    readResults ++= (1 to 10).pmap(threadId => {
      val startTime = System.currentTimeMillis
      (1 to 1000).foreach(i => ns.get(IntRec(scala.util.Random.nextInt(clusterSize * recsPerServer))))
      (ReadClient(clusterName, clientId, threadId), ReadPerfResult(10000, startTime, System.currentTimeMillis))
    })

    registerAndBlock("endRead")

    System.exit(0)
  }
}
