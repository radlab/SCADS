package edu.berkeley.cs
package scads
package consistency

import deploylib._
import deploylib.mesos._
import avro.marker._
import comm._
import config._
import storage._

import net.lag.logging.Logger

import edu.berkeley.cs.scads.storage.transactions._
import edu.berkeley.cs.scads.storage.transactions.FieldAnnotations._
import edu.berkeley.cs.scads.perf._

case class Result(var hostname: String,
		  var timestamp: Long) extends AvroPair {
  var rows: Int = _
}

object Experiment extends ExperimentBase {
  def run(cluster1: deploylib.mesos.Cluster,
          cluster2: deploylib.mesos.Cluster): Unit = {
    Task().schedule(resultClusterAddress, cluster1, cluster2)
  }
}

case class KeyRec(var x: Int) extends AvroRecord

case class ValueRec(var s: String,
                    @FieldGT(1)
                    @FieldGE(2)
                    @FieldLT(3)
                    @FieldLE(4)
                    var i: Int,
                    @FieldGT(1)
                    @FieldGE(1)
                    @FieldLT(4)
                    @FieldLE(4)
                    var a: Long,
                    var b: Float,
                    var c: Double) extends AvroRecord

case class Task(var replicationFactor: Int = 2)
     extends AvroTask with AvroRecord with TaskBase {
  
  var resultClusterAddress: String = _
  var clusterAddress: String = _
  var numDC: Int = _

  def schedule(resultClusterAddress: String, cluster1: deploylib.mesos.Cluster,
               cluster2: deploylib.mesos.Cluster): Unit = {
    this.resultClusterAddress = resultClusterAddress
    this.numDC = 2

    val scadsCluster = newMDCCScadsCluster(2, cluster1, cluster2)

    clusterAddress = scadsCluster.root.canonicalAddress

    val task1 = this.toJvmTask(cluster1.classSource)
    cluster1.serviceScheduler.scheduleExperiment(task1 :: Nil)
  }

  def run(): Unit = {
    val logger = Logger()
    val cluster = new ExperimentalScadsCluster(ZooKeeperNode(clusterAddress))
    cluster.blockUntilReady(replicationFactor * numDC)

    println("cluster.getAvailableServers: " + cluster.getAvailableServers)

//    val resultCluster = new ScadsCluster(ZooKeeperNode(resultClusterAddress))
//    val results = resultCluster.getNamespace[Result]("singleDataCenterTest")

    val ns = new SpecificNamespace[KeyRec, ValueRec]("testns", cluster, cluster.namespaces) with Transactions[KeyRec, ValueRec] {
      override val protocolType = TxProtocol2pc()
    }
    ns.open()
    ns.setPartitionScheme(List((None, cluster.getAvailableServers)))

    ns.put(KeyRec(1), ValueRec("A", 1, 1, 1.0.floatValue, 1.0))
    ns.put(KeyRec(2), ValueRec("B", 1, 1, 1.0.floatValue, 1.0))
    ns.put(KeyRec(3), ValueRec("C", 1, 1, 1.0.floatValue, 1.0))
    ns.put(KeyRec(4), ValueRec("D", 1, 1, 1.0.floatValue, 1.0))

    val tx1 = new Tx(100) ({
      List.range(5, 5 + 4).foreach(x => ns.put(KeyRec(x),
                                               ValueRec("G", 1, 1, 1.0.floatValue, 1.0)))
    }).Execute()

    val tx2 = new Tx(100) ({
      List.range(9, 9 + 4).foreach(x => ns.put(KeyRec(x),
                                               ValueRec("H", 1, 1, 1.0.floatValue, 1.0)))
    }).Execute()

    val tx3 = new Tx(100) ({
      List.range(7, 7 + 4).foreach(x => ns.put(KeyRec(x),
                                               ValueRec("I", 1, 1, 1.0.floatValue, 1.0)))
    }).Execute()

    println("delete 1")
    val tx4 = new Tx(100) ({
      // need to read your writes...
      ns.get(KeyRec(1))
      ns.put(KeyRec(1), None)
    }).Execute()

    println("running some logical updates")
    val tx5 = new Tx(100) ({
      ns.putLogical(KeyRec(12), ValueRec("", 2, 3, 2.1.floatValue, 0.2))
    })
    tx5.Execute()
    tx5.Execute()
    tx5.Execute()
    tx5.Execute()
    tx5.Execute()
    ns.getRange(None, None).foreach(x => println(x))

//    val hostname = java.net.InetAddress.getLocalHost.getHostName
//    val result = Result(hostname, System.currentTimeMillis)
//    result.rows = ns.getRange(None, None).size
//    results.put(result)
  }
}
