package edu.berkeley.cs.scads
package piql
package mviews

import net.lag.logging.Logger

import comm._
import config._
import storage._
import exec._
import perf._
import deploylib._
import deploylib.mesos._

import scala.math._
import scala.util.Random
import scala.collection.mutable.HashSet
import scala.collection.mutable.HashMap

/* testing client */
class MVTest(val cluster: ScadsCluster, val client: TagClient) {
  protected val logger = Logger("edu.berkeley.cs.scads.piql.mviews.MVTest")
  val tagA = "tagA"
  val tagB = "tagB"
  var populated = false

  def pessimalScaleup(n: Int) = {
    assert (!populated)

    var tags = List[Tuple2[String,String]]()
    for (i <- 0 until n) {
      var item = "item%%0%dd".format(("" + n).length).format(i)
      tags ::= (item, tagA)
    }

    tags ::= ("itemZ", tagB)
    tags ::= ("itemZ", tagA)
    client.initBulk(tags)

    populated = true
  }

  def doPessimalFetch() = {
    val start = System.nanoTime / 1000
    val res = client.selectTags(tagA, tagB)
    assert (res.length == 1)
    System.nanoTime / 1000 - start
  }

  def reset() = {
    logger.info("resetting data...")
    client.clear()
    populated = false
  }
}

/* convenient test configurations */
object MVTest extends ExperimentBase {
  val suffix = "pessimalResults"
  override val resultCluster = new ScadsCluster(ZooKeeperNode(relativeAddress(MVResult.suffix)))
  val results = resultCluster.getNamespace[MVResult](MVResult.suffix)

  def newNaive(): MVTest = {
    val cluster = TestScalaEngine.newScadsCluster(3)
    val client = new NaiveTagClient(cluster, new SimpleExecutor)
    new MVTest(cluster, client)
  }

  def newM(): MVTest = {
    val cluster = TestScalaEngine.newScadsCluster(3)
    val client = new MTagClient(cluster, new SimpleExecutor)
    new MVTest(cluster, client)
  }

  def go(implicit cluster: deploylib.mesos.Cluster, classSource: Seq[ClassSource]): Unit = {
    new Task().schedule(relativeAddress(MVResult.suffix))
  }
}
