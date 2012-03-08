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
import storage.transactions._

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

  /* returns string form of point in keyspace at k/max */
  private def ksSeek(i: Int, max: Int): String = {
    assert (i < max)
    "%013d".format(ksMax * i / max)
  }

  /* returns serialized key in keyspace at k/max */
  def serializedPointInKeyspace(k: Int, max: Int): Option[Array[Byte]] = {
    assert (k > 0 && k < max)
    Some(client.tagToBytes(ksSeek(k, max)))
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
        val item = ksSeek(i, totalItems)
        tagsof(item) = new HashSet[String]()
      }
    }

    /* only generate items in our segment */
    for (i <- 1 to (tagsToGenerate/numSegments)) {
      var item = randomItemInSegment(segment, numSegments)
      var tag = randomTag
      while (tagsof(item).size > maxTagsPerItem) {
        item = randomItemInSegment(segment, numSegments)
      }
      while (tagsof(item).contains(tag)) {
        tag = randomTag
      }
      tagsof(item).add(tag)
      bulk ::= (item, tag)
    }

    tagsof.clear
    client.initBulk(bulk)
  }

  /* whether item at index i is in segment */
  private def inSegment(i: Int, segment: Int, ns: Int): Boolean = {
    i % ns == segment
  }

  private def randomTag(implicit rnd: Random) = {
    ksSeek(rnd.nextInt(uniqueTags), uniqueTags)
  }

  private def randomItemInSegment(segment: Int, ns: Int)(implicit rnd: Random) = {
    def adjust(i: Int): Int = segment + i - (i % ns)
    var a = adjust(rnd.nextInt(totalItems))

    /* re-randomize in rare edge cases*/
    while (!inSegment(a, segment, ns)) {
      a = adjust(rnd.nextInt(totalItems))
    }

    ksSeek(a, totalItems)
  }

  private def randomItem(implicit rnd: Random) = {
    ksSeek(rnd.nextInt(totalItems), totalItems)
  }

  def randomGet(implicit rnd: Random) = {
    val start = System.nanoTime / 1000
    val tag1 = randomTag
    val tag2 = randomTag
    client.fastSelectTags(tag1, tag2)
    System.nanoTime / 1000 - start
  }

  def randomPut(limit: Int)(implicit rnd: Random): Tuple2[Long,Long] = {
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
    client.addTag(item, tag)
  }

  def randomPutTxn(limit: Int)(implicit rnd: Random): Tuple2[Long,Long] = {
    var start: Long = System.nanoTime / 1000
    new Tx(1000) ({
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
      start = System.nanoTime / 1000
      client.addTag(item, tag)
    }).Execute()
    (-1, System.nanoTime / 1000 - start)
  }

  def randomDel(implicit rnd: Random): Tuple2[Long,Long] = {
    var item = randomItem
    var assoc = client.selectItem(item)
    var tries = 0
    while (assoc.length == 0 && tries < 7) {
      item = randomItem
      assoc = client.selectItem(item)
      tries += 1
    }
    assert (assoc.length > 0)
    val a = assoc(rnd.nextInt(assoc.length))
    client.removeTag(item, a)
  }

  def randomDelTxn(implicit rnd: Random): Tuple2[Long,Long] = {
    var res: Tuple2[Long,Long] = (-1, -1)
    new Tx(1000) ({
      res = randomDel
    }).Execute()
    res
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
  val scaled = rc.getNamespace[ParResult3]("ParResult3")
  implicit val exec = new ParallelExecutor

  def newNaive(): MVPessimalTest = {
    val cluster = TestScalaEngine.newScadsCluster(3)
    val client = new NaiveTagClient(cluster, exec)
    new MVPessimalTest(cluster, client)
  }

  def newM(): MVScaleTest = {
    val cluster = TestScalaEngine.newScadsCluster(3)
    val client = new MTagClient(cluster, exec)
    new MVScaleTest(cluster, client, 10, 40, 100, 200)
  }

  def summarize(comment: String, partitions: Int) = {
    val data = scaled.iterateOverRange(None,None).toList.filter(x => x.comment.equals(comment) && x.partitions == partitions && x.iteration > 1)
    val totals = data.map(x =>
      (x.getTimes,
       x.putTimes,
       x.nvputTimes,
       (x.getTimes.totalRequests + x.putTimes.totalRequests + x.delTimes.totalRequests)*1.0/x.runTimeMs*1000))
      .reduceLeft((x,y) => (x._1 + y._1, x._2 + y._2, x._3 + y._3, x._4 + y._4))
    ("getl=" + (totals._1.quantile(0.99)/1000.0),
     "putl=" + (totals._2.quantile(0.99)/1000.0),
     "nvputl=" + (totals._3.quantile(0.99)/1000.0),
     "ops/s=" + (totals._4/(data.length.doubleValue/partitions)),
     "runs=" + (data.length.doubleValue/partitions))
  }

  def goPessimal(implicit cluster: deploylib.mesos.Cluster, classSource: Seq[ClassSource]): Unit = {
    new Task().schedule(relativeAddress(Results.suffix))
  }

  def go(replicas: Int = 1, partitions: Int = 8, nClients: Int = 8, itemsPerMachine: Int = 100000, threadCount: Int = 32, comment: String = "")(implicit cluster: deploylib.mesos.Cluster, classSource: Seq[ClassSource]): Unit = {
    new ScaleTask(replicas=replicas, partitions=partitions, nClients=nClients, itemsPerMachine=itemsPerMachine, threadCount=threadCount, comment = comment).schedule(relativeAddress(Results.suffix))
  }
}
