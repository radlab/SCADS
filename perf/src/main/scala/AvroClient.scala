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

  def newCluster(implicit classpath: Seq[ClassSource], scheduler: ExperimentScheduler, zookeeperRoot: ZooKeeperProxy#ZooKeeperNode): ScadsCluster = {
    val (tasks, cluster) = delayedCluster
    scheduler.scheduleExperiment(tasks)
    cluster
  }

  def delayedCluster(implicit classpath: Seq[ClassSource], zookeeperRoot: ZooKeeperProxy#ZooKeeperNode): (Seq[JvmTask], ScadsCluster) = {
    val clusterRoot = zookeeperRoot.getOrCreate("scads").createChild("experimentCluster", mode = CreateMode.PERSISTENT_SEQUENTIAL)
    clusterAddress = clusterRoot.canonicalAddress
    val serverProcs = List.fill(numServers)(ScalaEngineTask(clusterAddress=clusterRoot.canonicalAddress).toJvmTask)
    val loaderProcs = List.fill(numLoaders)(this.toJvmTask)

    (serverProcs ++ loaderProcs, new ScadsCluster(clusterRoot))
  }

  // scadsClusterRoot is the actual root of the cluster, and should already be
  // created before calling this method.
  def getLoadingTasks(implicit classpath: Seq[ClassSource], scadsClusterRoot: ZooKeeperProxy#ZooKeeperNode): Seq[JvmTask] = {
    clusterAddress = scadsClusterRoot.canonicalAddress
    val loaderProcs = List.fill(numLoaders)(this.toJvmTask)
    loaderProcs
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

  def delayedSchedule(cluster: ScadsCluster, resultCluster: ScadsCluster)(implicit classpath: Seq[ClassSource], zookeeperRoot: ZooKeeperProxy#ZooKeeperNode): Seq[JvmTask] = {
    val experimentRoot = zookeeperRoot.getOrCreate("experiments").createChild("experiment", mode = CreateMode.PERSISTENT_SEQUENTIAL)
    experimentAddress = experimentRoot.canonicalAddress
    clusterAddress = cluster.root.canonicalAddress
    resultClusterAddress = resultCluster.root.canonicalAddress

    val task = this.toJvmTask
    Array.fill(numClients)(task)
  }

  // scadsClusterRoot is the actual root of the cluster, and should already be
  // created before calling this method.
  def getExperimentTasks(implicit classpath: Seq[ClassSource], scadsClusterRoot: ZooKeeperProxy#ZooKeeperNode, resultClusterAddress: String): Seq[JvmTask] = {
    val experimentRoot = scadsClusterRoot.getOrCreate("experiments").createChild("experiment", mode = CreateMode.PERSISTENT_SEQUENTIAL)
    experimentAddress = experimentRoot.canonicalAddress
    clusterAddress = scadsClusterRoot.canonicalAddress
    this.resultClusterAddress = resultClusterAddress

    val task = this.toJvmTask
    Array.fill(numClients)(task)
  }

  def schedule(cluster: ScadsCluster, resultCluster: ScadsCluster)(implicit classpath: Seq[ClassSource], scheduler: ExperimentScheduler, zookeeperRoot: ZooKeeperProxy#ZooKeeperNode): this.type = {
    scheduler.scheduleExperiment(delayedSchedule(cluster, resultCluster))
    this
  }
}
