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

abstract trait DataLoadingTask extends AvroTask {
  var numServers: Int
  var numLoaders: Int

  var clusterAddress: String 

  def newCluster(implicit classpath: Seq[ClassSource], scheduler: ExperimentScheduler, zookeeperRoot: ZooKeeperProxy#ZooKeeperNode): ScadsCluster = {
    val clusterRoot = zookeeperRoot.getOrCreate("scads").createChild("experimentCluster", mode = CreateMode.PERSISTENT_SEQUENTIAL)
    clusterAddress = clusterRoot.canonicalAddress
    val serverProcs = List.fill(numServers)(ScalaEngineTask(clusterAddress=clusterRoot.canonicalAddress).toJvmTask)
    val loaderProcs = List.fill(numLoaders)(this.toJvmTask)

    scheduler.scheduleExperiment(serverProcs ++ loaderProcs)

    new ScadsCluster(clusterRoot)
  }
}

abstract trait ReplicatedExperimentTask extends AvroTask {
  var numClients: Int
  var experimentAddress: String

  def schedule(implicit classpath: Seq[ClassSource], scheduler: ExperimentScheduler, zookeeperRoot: ZooKeeperProxy#ZooKeeperNode): Unit = {
    val experimentRoot = zookeeperRoot.getOrCreate("experiments").createChild("experiment", mode = CreateMode.PERSISTENT_SEQUENTIAL)
    experimentAddress = experimentRoot.canonicalAddress

    val task = this.toJvmTask
    scheduler.scheduleExperiment(Array.fill(numClients)(task))
  }
}
