package deploylib
package mesos

import ec2._

import org.apache.zookeeper.CreateMode
import edu.berkeley.cs.scads.comm.ZooKeeperNode
import edu.berkeley.cs.avro.marker.AvroRecord

case class TestTask(var syncNodeAddress: String) extends AvroTask with AvroRecord {
  def run() = {
    val syncNode = ZooKeeperNode(syncNodeAddress)
    syncNode.createChild("done")
  }
}

object MesosTest {
  implicit val cluster = new Cluster(USEast1)
  implicit val classSource = cluster.classSource

  def run: Unit = {
    cluster.setup()
    runTask()
    //cluster.stopAllInstances
  }

  def runTask(cluster: Cluster = cluster): Unit = {
    val syncNode = cluster.zooKeeperRoot.createChild("syncNode", mode=CreateMode.PERSISTENT_SEQUENTIAL)
    cluster.serviceScheduler.scheduleExperiment(TestTask(syncNode.canonicalAddress).toJvmTask :: Nil)
    syncNode.awaitChild("done")
  }
}