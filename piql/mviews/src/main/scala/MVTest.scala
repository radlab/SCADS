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

  /* tags used for pessimal scaleup */
  val tagA = "tagA"
  val tagB = "tagB"
  var populated = false

  /* tags, items used for populate/MVScale */
  var tags: Array[String] = _
  var items: Array[String] = _
  val ksMin = 0
  val ksMax = 1e12.longValue

  /* converts keyspace index to string */
  def ksToTag(k: Long): String = {
    assert (k >= ksMin && k <= ksMax)
    "%013d".format(k)
  }

  /* returns point in keyspace at k/max */
  def ksSample(i: Int, max: Int): Long = {
    assert (i < max)
    return ksMax * i / max
  }

  /* returns serialized key in keyspace at k/max */
  def indexKeyspace(k: Int, max: Int): Option[Array[Byte]] = {
    assert (k > 0 && k < max)
    Some(client.tagToBytes(ksToTag(ksSample(k, max))))
  }

  /* for MVScale */
  def randomPopulate(nitems: Int, meanTags: Int, maxTags: Int) = {
    assert (!populated && nitems > 1 && meanTags < maxTags)

    val tags = populate(nitems, 400, meanTags * nitems, maxTags)
    // 1. create nitems, n*meanTags tags
    // 2. randomly assign items to tags

    populated = true
  }

  def populate(nitems: Int,
               unique_tags: Int,
               ntags: Int,
               max_tags_per_item: Int) = {
    assert (!populated)
    assert (max_tags_per_item * nitems > ntags)
    assert (unique_tags > max_tags_per_item)

    tags = new Array[String](unique_tags)
    items = new Array[String](nitems)
    val tagsof = new HashMap[String,HashSet[String]]()

    for (i <- Range(0, unique_tags))  {
      tags(i) = ksToTag(ksSample(i, unique_tags))
    }

    for (i <- Range(0, nitems)) {
      val item = ksToTag(ksSample(i, nitems))
      items(i) = item
      tagsof(item) = new HashSet[String]()
    }

    var bulk = List[Tuple2[String,String]]()
    for (i <- 1 to ntags) {
      var item = items(Random.nextInt(items.length))
      var tag = tags(Random.nextInt(tags.length))
      if (i % 100 == 0) {
        logger.info("Adding tags: %d/%d", i, ntags)
      }
      while (tagsof(item).size > max_tags_per_item) {
        item = items(Random.nextInt(items.length))
      }
      while (tagsof(item).contains(tag)) {
        tag = tags(Random.nextInt(tags.length))
      }
      tagsof(item).add(tag)
      bulk ::= (item, tag)
    }

    client.initBulk(bulk)

    populated = true
  }

  /* for MVScale */
  def randomAction() = {
    val start = System.nanoTime / 1000
    assert (false)
    System.nanoTime / 1000 - start
  }

  /* for MVTask */
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

  /* for MVTask */
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
