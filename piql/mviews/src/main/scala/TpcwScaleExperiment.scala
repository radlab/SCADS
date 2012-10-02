package edu.berkeley.cs
package scads
package piql
package mviews

import deploylib.mesos._
import tpcw._
import comm._
import storage._
import piql.debug.DebugExecutor

object TpcwScaleExperiment {
  var resultClusterAddress = ZooKeeperNode("zk://zoo1.millennium.berkeley.edu,zoo2.millennium.berkeley.edu,zoo3.millennium.berkeley.edu/home/marmbrus/sigmod2013")
  val cluster = new Cluster()
  implicit def zooKeeperRoot = cluster.zooKeeperRoot
  val resultsCluster = new ScadsCluster(resultClusterAddress)

  implicit def classSource = cluster.classSource
  implicit def serviceScheduler = cluster.serviceScheduler

  implicit def toOption[A](a: A) = Option(a)

  lazy val testTpcwClient =
    new piql.tpcw.TpcwClient(new piql.tpcw.TpcwLoaderTask(10,5,10,10000,2).newTestCluster, new ParallelExecutor with DebugExecutor)
}