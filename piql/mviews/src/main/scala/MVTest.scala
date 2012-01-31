package edu.berkeley.cs.scads
package piql
package mviews

import net.lag.logging.Logger

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
  var populated = false
  var tags: Array[String] = _
  var items: Array[String] = _

  /* somewhat arbitrary population of tags based on a single parameter */
  def scaleup(scale: Int,
              tag_item_ratio: Double = 4.0,
              item_tag_limit: Int = 10): Tuple2[Array[String],Array[String]] = {
    Random.setSeed(0)
    populate(
      scale,
      item_tag_limit + sqrt(scale).round.asInstanceOf[Int],
      (tag_item_ratio * scale).round.asInstanceOf[Int],
      item_tag_limit
    )
  }

  def getRandomPair(): Long = {
    assert (populated)
    assert (tags.length > 1)
    var tag1 = tags(Random.nextInt(tags.length))
    var tag2 = tags(Random.nextInt(tags.length))
    while (tag1 == tag2) {
      tag2 = tags(Random.nextInt(tags.length))
    }
    val start = System.nanoTime / 1000
    val res = client.selectTags(tag1, tag2)
    assert (res != null)
    System.nanoTime / 1000 - start
  }

  def depopulate() = {
    if (populated) {
      logger.info("resetting data...")
      client.clear()
      tags = Array[String]()
      items = Array[String]()
      populated = false
    } else {
      logger.info("not populated")
    }
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
      tags(i) = "tag:" + Random.nextInt.toString
    }

    for (i <- Range(0, nitems)) {
      val item = "item:" + Random.nextInt.toString
      items(i) = item
      tagsof(item) = new HashSet[String]()
    }

    for (i <- 1 to ntags) {
      var item = items(Random.nextInt(items.length))
      var tag = tags(Random.nextInt(tags.length))
      if (i % 100 == 0) {
        logger.info("Adding tags: " + i)
      }
      while (tagsof(item).size > max_tags_per_item) {
        item = items(Random.nextInt(items.length))
      }
      while (tagsof(item).contains(tag)) {
        tag = tags(Random.nextInt(tags.length))
      }
      tagsof(item).add(tag)
      client.addTag(item, tag)
    }

    populated = true
    (tags, items)
  }

  def doRandomFetch(tags: Array[String]) = {
    val tag1 = tags(Random.nextInt(tags.length))
    var tag2 = tags(Random.nextInt(tags.length))
    while (tag1 == tag2) {
      tag2 = tags(Random.nextInt(tags.length))
    }
    val start = System.nanoTime / 1000
    client.selectTags(tag1, tag2)
    System.nanoTime / 1000 - start
  }
}

/* convenient test configurations */
object MVTest extends ExperimentBase {
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
    new Task().schedule(relativeAddress("expResults"))
  }
}
