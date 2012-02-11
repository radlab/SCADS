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

/* for testing client at scale */
class MVScaleTest(val cluster: ScadsCluster, val client: TagClient,
                  val totalItems: Int, val tagsToGenerate: Int,
                  val maxTagsPerItem: Int, val uniqueTags: Int) {
  protected val logger = Logger("edu.berkeley.cs.scads.piql.mviews.MVScaleTest")

  /* fixed parameters */
  val ksMin = 0
  val ksMax = 1e12.longValue

  /* converts keyspace index to string */
  private def ksToString(k: Long): String = {
    assert (k >= ksMin && k <= ksMax)
    "%013d".format(k)
  }

  /* returns point in keyspace at k/max */
  private def ksSample(i: Int, max: Int): Long = {
    assert (i < max)
    return ksMax * i / max
  }

  /* returns serialized key in keyspace at k/max */
  def serializedPointInKeyspace(k: Int, max: Int): Option[Array[Byte]] = {
    assert (k > 0 && k < max)
    Some(client.tagToBytes(ksToString(ksSample(k, max))))
  }

  /* distributed data load task by segment
     we can do this since the materialized view
     has no item-to-item dependencies */
  def populateSegment(segment: Int, numSegments: Int) = {
    assert (segment >= 0 && segment < numSegments)
    assert (maxTagsPerItem * totalItems > tagsToGenerate)
    assert (uniqueTags > maxTagsPerItem)
    logger.info("Loading segment " + segment)

    implicit val rnd = new Random()
    val tagsof = new HashMap[String,HashSet[String]]()
    var bulk = List[Tuple2[String,String]]()

    for (i <- Range(0, totalItems)) {
      if (inSegment(i, segment, numSegments)) {
        val item = ksToString(ksSample(i, totalItems))
        tagsof(item) = new HashSet[String]()
      }
    }

    /* only generate items in our segment */
    for (i <- 1 to (tagsToGenerate/numSegments)) {
      var item = randomItemInSegment(segment, numSegments)
      var tag = randomTag
      while (tagsof(item).size > maxTagsPerItem) {
        randomItemInSegment(segment, numSegments)
      }
      while (tagsof(item).contains(tag)) {
        tag = randomTag
      }
      tagsof(item).add(tag)
      bulk ::= (item, tag)
    }

    client.initBulk(bulk)
  }

  /* whether item at index i is in segment */
  private def inSegment(i: Int, segment: Int, numSegments: Int): Boolean = {
    i % numSegments == segment
  }

  private def randomTag(implicit rnd: Random) = {
    ksToString(ksSample(rnd.nextInt(uniqueTags), uniqueTags))
  }

  private def randomItemInSegment(segment: Int, numSegments: Int)(implicit rnd: Random) = {
    var i = rnd.nextInt(totalItems)
    // TODO maybe less brutish way
    while (!inSegment(i, segment, numSegments)) {
      i = rnd.nextInt(totalItems)
    }
    ksToString(ksSample(i, totalItems))
  }

  private def randomItem(implicit rnd: Random) = {
    ksToString(ksSample(rnd.nextInt(totalItems), totalItems))
  }

  def randomGet(implicit rnd: Random) = {
    val start = System.nanoTime / 1000
    val tag1 = randomTag
    val tag2 = randomTag
    client.fastSelectTags(tag1, tag2)
    System.nanoTime / 1000 - start
  }

  def randomPut(limit: Int)(implicit rnd: Random) = {
    var item = randomItem
    var tag = randomTag
    var tries = 0
    def hasTag(item: String, tag: String) = {
      val assoc = client.selectItem(item)
      assoc.length >= limit || assoc.contains(tag)
    }
    while (hasTag(item, tag) && tries < 7) {
      item = randomItem
      tag = randomTag
      tries += 1
    }
    assert (!hasTag(item, tag))
    val start = System.nanoTime / 1000
    client.addTag(item, tag, limit)
    System.nanoTime / 1000 - start
  }

  def randomDel(implicit rnd: Random) = {
    var item = randomItem
    var assoc = client.selectItem(item)
    var tries = 0
    while (assoc.length == 0 && tries < 7) {
      item = randomItem
      assoc = client.selectItem(item)
      tries += 1
    }
    assert (assoc.length > 0)
    val start = System.nanoTime / 1000
    val a = assoc(rnd.nextInt(assoc.length))
    client.removeTag(item, a)
    System.nanoTime / 1000 - start
  }
}

class MVPessimalTest(val cluster: ScadsCluster, val client: TagClient) {
  protected val logger = Logger("edu.berkeley.cs.scads.piql.mviews.MVPessimalTest")
  var populated = false

  /* tags used for pessimal scaleup */
  val tagA = "tagA"
  val tagB = "tagB"

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
  val rc = new ScadsCluster(ZooKeeperNode(relativeAddress(Results.suffix)))
  val pessimal = rc.getNamespace[MVResult]("MVResult")
  val scaled = rc.getNamespace[ParResult]("ParResult")

  def newNaive(): MVPessimalTest = {
    val cluster = TestScalaEngine.newScadsCluster(3)
    val client = new NaiveTagClient(cluster, new SimpleExecutor)
    new MVPessimalTest(cluster, client)
  }

  def newM(): MVPessimalTest = {
    val cluster = TestScalaEngine.newScadsCluster(3)
    val client = new MTagClient(cluster, new SimpleExecutor)
    new MVPessimalTest(cluster, client)
  }

  def goPessimal(implicit cluster: deploylib.mesos.Cluster, classSource: Seq[ClassSource]): Unit = {
    new Task().schedule(relativeAddress(Results.suffix))
  }

  def go(implicit cluster: deploylib.mesos.Cluster, classSource: Seq[ClassSource]): Unit = {
    new ScaleTask().schedule(relativeAddress(Results.suffix))
  }
}
