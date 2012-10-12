package edu.berkeley.cs
package scads
package perf

import comm._
import storage._
import avro.runtime._
import avro.marker._
import deploylib.mesos._

import net.lag.logging.Logger
import org.apache.avro.generic.IndexedRecord
import org.apache.zookeeper.CreateMode

trait ExperimentTask extends AvroTask {
  protected def retry[ReturnType](tries: Int = 5)(func: => ReturnType): ReturnType = {
    try func catch {
      case e if(tries > 0) => {
	logger.warning(e, "Exception caught, trying %d more times", tries)
	retry(tries - 1)(func)
      }
    }
  }
}

abstract trait DataLoadingTask extends ExperimentTask {
  var numServers: Int
  var numLoaders: Int

  var clusterAddress: String 

  lazy val clusterRoot = ZooKeeperNode(clusterAddress)

  def newTestCluster(): ScadsCluster = {
    val cluster = TestScalaEngine.newScadsCluster(numServers)
    clusterAddress = cluster.root.canonicalAddress
    val threads = (1 to numLoaders).map(i => new Thread(this, "TestDataLoadingTask " + i))
    threads.foreach(_.start)
    cluster.root.awaitChild("clusterReady")
    cluster
  }

  def newCluster(implicit cluster: Cluster): ScadsCluster = {
    val (tasks, scadsCluster) = delayedCluster
    cluster.serviceScheduler.scheduleExperiment(tasks)
    scadsCluster
  }

  def delayedCluster(implicit cluster: Cluster): (Seq[JvmTask], ScadsCluster) = {
    val clusterRoot = cluster.zooKeeperRoot.getOrCreate("scads").createChild("experimentCluster", mode = CreateMode.PERSISTENT_SEQUENTIAL)
    clusterAddress = clusterRoot.canonicalAddress
    val serverProcs = List.fill(numServers)(ScalaEngineTask(clusterAddress=clusterRoot.canonicalAddress, noSync=true).toJvmTask)
    val loaderProcs = List.fill(numLoaders)(this.toJvmTask)

    (serverProcs ++ loaderProcs, new ScadsCluster(clusterRoot))
  }
}

abstract trait ReplicatedExperimentTask extends ExperimentTask {
  var numClients: Int
  var resultClusterAddress: String
  var clusterAddress: String
  var experimentAddress: String

  protected lazy val clusterRoot = ZooKeeperNode(clusterAddress)
  protected lazy val coordination = clusterRoot.getOrCreate("coordination/clients")
  protected lazy val cluster = new ScadsCluster(clusterRoot)
  protected lazy val resultCluster = new ScadsCluster(ZooKeeperNode(resultClusterAddress))

  def testLocally(cluster: ScadsCluster) = {
    val experimentRoot = cluster.root.getOrCreate("experiments").createChild("experiment", mode = CreateMode.PERSISTENT_SEQUENTIAL)
    experimentAddress = experimentRoot.canonicalAddress
    clusterAddress = cluster.root.canonicalAddress
    resultClusterAddress = cluster.root.canonicalAddress

    val threads = (1 to numClients).map(_ => new Thread(this))
    threads.foreach(_.start)
    threads
  }

  def delayedSchedule(scadsCluster: ScadsCluster, resultCluster: ScadsCluster)(implicit cluster: Cluster): Seq[JvmTask] = {
    val experimentRoot = cluster.zooKeeperRoot.getOrCreate("experiments").createChild("experiment", mode = CreateMode.PERSISTENT_SEQUENTIAL)
    experimentAddress = experimentRoot.canonicalAddress
    clusterAddress = scadsCluster.root.canonicalAddress
    resultClusterAddress = resultCluster.root.canonicalAddress

    val task = this.toJvmTask
    Array.fill(numClients)(task)
  }

  def schedule(scadsCluster: ScadsCluster, resultCluster: ScadsCluster)(implicit cluster: Cluster): this.type = {
    cluster.serviceScheduler.scheduleExperiment(delayedSchedule(scadsCluster, resultCluster))
    this
  }
}
