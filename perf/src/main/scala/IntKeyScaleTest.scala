package edu.berkeley.cs.scads.perf

import edu.berkeley.cs.scads.comm._
import edu.berkeley.cs.scads.mesos._
import edu.berkeley.cs.scads.storage._
import com.googlecode.avro.runtime._

import org.apache.zookeeper.CreateMode

object IntKeyScaleTest extends optional.Application {
  implicit val zooRoot = RClusterZoo.root
  implicit def toOption[A](a: A) = Option(a)

  def main(clusterSize: Int, recsPerServer: Int = 100): Unit = {
    println("Begining cluster scale test with " + clusterSize + " servers.")
    val cluster = ScadsMesosCluster(clusterSize)
    val coordination = cluster.root.getOrCreate("coordination")
    cluster.blockTillReady

    val procDesc = JvmProcess(
      "/work/marmbrus/mesos/perf-2.1.0-SNAPSHOT.jar:/work/marmbrus/mesos:/work/marmbrus/mesos/mesos-scads-2.1.0-SNAPSHOT-jar-with-dependencies.jar",
      "edu.berkeley.cs.scads.perf.IntKeyScaleClient",
      "--clusterSize" :: clusterSize.toString :: "--recsPerServer" :: recsPerServer.toString ::"--clusterName" :: cluster.root.name :: Nil)

    val clients = new ServiceScheduler("IntKey Scale Test")
    (1 to clusterSize).foreach(i => clients.runService(512, 1, procDesc))

    println("Waiting for clients to start")
    coordination.getOrCreate("startWrite").awaitChild("client", clusterSize - 1)
    println("Waiting for writing to complete")
    coordination.getOrCreate("endWrite").awaitChild("client", clusterSize - 1)
    println("Test Done")

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

    def registerAndBlock(name: String):Int  = {
      val node = coordination.getOrCreate(name)
      val seqNum = node.createChild("client", mode = CreateMode.EPHEMERAL_SEQUENTIAL).sequenceNumber
      node.awaitChild("client", clusterSize - 1)
      seqNum
    }

    val clientId = registerAndBlock("startWrite")

    val startKey = clientId * recsPerServer
    val endKey = (clientId + 1) * recsPerServer
    (startKey to endKey).foreach(i => ns.put(IntRec(i), IntRec(i)))

    registerAndBlock("endWrite")

    System.exit(0)
  }
}
