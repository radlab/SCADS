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

import edu.berkeley.cs.scads.storage.FieldAnnotations._

case class Result(var hostname: String,
		  var timestamp: Long) extends AvroPair {
  var rows: Int = _
}

object Experiment extends ExperimentBase {

  def reset(implicit cluster: deploylib.mesos.Cluster, classSource: Seq[ClassSource]) = {
    results.delete()
    results.open()
    cluster.restart
  }

  lazy val results = resultCluster.getNamespace[Result]("singleDataCenterTest")
  def goodResults = results.iterateOverRange(None,None)


  def main(args: Array[String]): Unit = {
//     val cluster = TestScalaEngine.newScadsCluster(2)
//     val task = new Task(cluster.root.canonicalAddress,
//			  cluster.root.canonicalAddress).run()

//    graphPoints.toSeq.sortBy(r => (r._2, r._1)).foreach(println)
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

  def schedule(resultClusterAddress: String)(implicit cluster: deploylib.mesos.Cluster, classSource: Seq[ClassSource]): Unit = {
    val scadsCluster = newScadsCluster(2)
    clusterAddress = scadsCluster.root.canonicalAddress
    this.resultClusterAddress = resultClusterAddress
    val task = this.toJvmTask
    cluster.serviceScheduler.scheduleExperiment(task :: Nil)
  }

  def run(): Unit = {
    val logger = Logger()
    val cluster = new ExperimentalScadsCluster(ZooKeeperNode(clusterAddress))
    cluster.blockUntilReady(replicationFactor)

    val resultCluster = new ScadsCluster(ZooKeeperNode(resultClusterAddress))
    val results = resultCluster.getNamespace[Result]("singleDataCenterTest")


    val ns = new SpecificNamespace[KeyRec, ValueRec]("testns", cluster, cluster.namespaces) with Transactions[KeyRec, ValueRec] {
      override val protocolType = TxProtocol2pc()
    }
    ns.open()

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

    val hostname = java.net.InetAddress.getLocalHost.getHostName
    val result = Result(hostname, System.currentTimeMillis)
    result.rows = ns.getRange(None, None).size
    results.put(result)
  }
}
