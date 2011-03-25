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

  def newTestCluster(): ScadsCluster = {
    val cluster = TestScalaEngine.newScadsCluster(numServers)
    clusterAddress = cluster.root.canonicalAddress
    val threads = (1 to numLoaders).map(i => new Thread(this, "TestDataLoadingTask " + i))
    threads.foreach(_.start)
    cluster
  }

  def newCluster(implicit classpath: Seq[ClassSource], scheduler: ExperimentScheduler, zookeeperRoot: ZooKeeperProxy#ZooKeeperNode): ScadsCluster = {
    val clusterRoot = zookeeperRoot.getOrCreate("scads").createChild("experimentCluster", mode = CreateMode.PERSISTENT_SEQUENTIAL)
    clusterAddress = clusterRoot.canonicalAddress
    val serverProcs = List.fill(numServers)(ScalaEngineTask(clusterAddress=clusterRoot.canonicalAddress).toJvmTask)
    val loaderProcs = List.fill(numLoaders)(this.toJvmTask)

    scheduler.scheduleExperiment(serverProcs ++ loaderProcs)

    new ScadsCluster(clusterRoot)
  }
}

abstract trait ReplicatedExperimentTask extends ExperimentTask {
  var numClients: Int
  var resultClusterAddress: String
  var clusterAddress: String
  var experimentAddress: String

  def schedule(cluster: ScadsCluster, resultCluster: ScadsCluster)(implicit classpath: Seq[ClassSource], scheduler: ExperimentScheduler, zookeeperRoot: ZooKeeperProxy#ZooKeeperNode): this.type = {
    val experimentRoot = zookeeperRoot.getOrCreate("experiments").createChild("experiment", mode = CreateMode.PERSISTENT_SEQUENTIAL)
    experimentAddress = experimentRoot.canonicalAddress
    clusterAddress = cluster.root.canonicalAddress
    resultClusterAddress = resultCluster.root.canonicalAddress

    val task = this.toJvmTask
    scheduler.scheduleExperiment(Array.fill(numClients)(task))
    this
  }
}
