package edu.berkeley.cs
package scads
package piql
package mviews

import deploylib.mesos._
import tpcw._
import tpcw.scale._
import comm._
import storage._
import piql.debug.DebugExecutor
import avro.marker.AvroRecord
import edu.berkeley.cs.scads.perf.ReplicatedExperimentTask


case class TpcwViewRefreshTask(var experimentAddress: String,
                               var clusterAddress: String,
                               var resultClusterAddress: String) extends AvroRecord with AvroTask {
  def run() = {
    val clusterRoot = ZooKeeperNode(clusterAddress)
    logger.info("Waiting for experiment to start.")
    clusterRoot.awaitChild("clusterReady")
    logger.info("Cluster ready... entering refresh loop")



  }
}

object TpcwScaleExperiment {
  var resultClusterAddress = ZooKeeperNode("zk://zoo1.millennium.berkeley.edu,zoo2.millennium.berkeley.edu,zoo3.millennium.berkeley.edu/home/marmbrus/sigmod2013")
  val resultsCluster = new ScadsCluster(resultClusterAddress)
  val scaleResults =  resultsCluster.getNamespace[Result]("tpcwScaleResults")

  implicit def toOption[A](a: A) = Option(a)

  lazy val testTpcwClient =
    new piql.tpcw.TpcwClient(new piql.tpcw.TpcwLoaderTask(10,5,10,10000,2).newTestCluster, new ParallelExecutor with DebugExecutor)

  lazy val tinyTpcwClient =
    new piql.tpcw.TpcwClient(new piql.tpcw.TpcwLoaderTask(1,1,1,1000,1).newTestCluster, new ParallelExecutor with DebugExecutor)

  def resultsByAction = scaleResults
    .iterateOverRange(None,None)
    .filter(_.iteration != 1)
    .flatMap(_.times).toSeq
    .groupBy(_.name)
    .map { case (name, hists) => (name, hists.reduceLeft(_ + _).quantile(0.99)) }

  def runScaleTest(numServers: Int, executor: String = "edu.berkeley.cs.scads.piql.exec.ParallelExecutor")(implicit cluster: Cluster) = {
    val (scadsTasks, scadsCluster) = TpcwLoaderTask(numServers, numServers/2, replicationFactor=2, numEBs = 150 * numServers/2, numItems = 10000).delayedCluster

    val tpcwTasks = TpcwWorkflowTask(
      numServers/2,
      executor,
      iterations = 4,
      runLengthMin = 5
    ).delayedSchedule(scadsCluster, resultsCluster)

    cluster.serviceScheduler.scheduleExperiment(scadsTasks ++ tpcwTasks)
  }
}