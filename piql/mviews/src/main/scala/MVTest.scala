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
import scala.math.pow

import scala.math._
import scala.util.Random
import scala.collection.mutable.HashSet
import scala.collection.mutable.HashMap

/* for testing client at scale */
class MVScaleTest(val cluster: ScadsCluster, val client: TagClient,
                  val totalItems: Int, val tagsToGenerate: Int,
                  val maxTagsPerItem: Int, val uniqueTags: Int,
                  val zipf: Double) {
  protected val logger = Logger("edu.berkeley.cs.scads.piql.mviews.MVScaleTest")

  /* lower bound of keyspace, inclusive */
  val ksMin = 0
  /* upper bound of keyspace, exclusive */
  val ksMax = 1L<<42 

  /* returns string form of point in keyspace at k/max */
  private def ksSeek(i: Long, max: Long): String = {
    assert (i < max)
    val res = "%013d".format(ksMax / max * i)
    if (res.length != 13) {
      logger.error("too long: " + res)
      logger.error("val was: " + i)
      logger.error("max was: " + max)
      assert(false)
    }
    res
  }

  /* returns serialized key in keyspace at k/max */
  def serializedPointInKeyspace(k: Int, max: Int): Option[Array[Byte]] = {
    assert (k > 0 && k < max)
    Some(client.tagToBytes(ksSeek(k, max)))
  }

  def serializedTupleInKeyspace(k: Int, l: Int, kmax: Int, lmax: Int): Option[Array[Byte]] = {
    assert (k >= 0 && k < kmax)
    assert (l >= 0 && l < lmax)
    Some(client.tupleToBytes(ksSeek(k, kmax), ksSeek(l, lmax)))
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
      var tag = chooseRandomTag
      while (tagsof(item).size > maxTagsPerItem) {
        item = randomItemInSegment(segment, numSegments)
      }
      while (tagsof(item).contains(tag)) {
        tag = chooseRandomTag
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

  /* assumes user partitions under zipf assumption */
  private def chooseRandomTag(implicit rnd: Random) = {
    if (zipf > 0) {
      ksSeek(ZipfDistribution.sample(uniqueTags, zipf), uniqueTags)
    } else {
      ksSeek(rnd.nextInt(uniqueTags), uniqueTags)
    }
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
    val tag1 = chooseRandomTag
    val tag2 = chooseRandomTag
    client.fastSelectTags(tag1, tag2)
    System.nanoTime / 1000 - start
  }

  def randomPut(limit: Int)(implicit rnd: Random): Tuple2[Long,Long] = {
    var item = randomItem
    var tag = chooseRandomTag
    var tries = 0
    def hasTag(item: String, tag: String) = {
      val assoc = client.selectItem(item)
      assoc.length >= limit || assoc.contains(tag)
    }
    while (hasTag(item, tag) && tries < 7) {
      item = randomItem
      tag = chooseRandomTag
      tries += 1
    }
    if (tries >= 7) {
      assert (!hasTag(item, tag))
    }
    client.addTag(item, tag)
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
  lazy val rc = new ScadsCluster(ZooKeeperNode(relativeAddress(Results.suffix)))
  lazy val scaled = rc.getNamespace[ParResult3]("ParResult3")
  implicit val exec = new ParallelExecutor

  def newNaive(): MVPessimalTest = {
    val cluster = TestScalaEngine.newScadsCluster(3)
    val client = new NaiveTagClient(cluster, exec)
    new MVPessimalTest(cluster, client)
  }

  def newM(): MVScaleTest = {
    val cluster = TestScalaEngine.newScadsCluster(3)
    val client = new MTagClient(cluster, exec)
    new MVScaleTest(cluster, client, 10, 40, 100, 2000, 1)
  }

  def variance(seq: Seq[Double]): Double = {
      (seq.map(pow(_, 2)).reduce(_+_) / seq.length) - pow(seq.reduce(_+_) / seq.length, 2)
  }

  def stdev(seq: Seq[Double]): Double = pow(variance(seq), 0.5)

  def summarize(comment: String, hours: Int = 999999) = {
    scaled.iterateOverRange(None,None).toList.filter(x => x.comment.equals(comment) && x.iteration > 1 && x.fromLastHours(hours)).groupBy(t => (t.partitions, t.clientId)).map({ case ((n,id), data) =>
      val putTimes = data.map(_.putTimes.quantile(0.99)/1000.0)
      val nvPutTimes = data.map(_.nvputTimes.quantile(0.99)/1000.0)
      val ops = data.map(x => (x.getTimes.totalRequests + x.putTimes.totalRequests + x.delTimes.totalRequests)*1.0/x.runTimeMs*1000)
      val totals = data.map(x =>
        (x.getTimes,
         x.putTimes,
         x.nvputTimes,
         x.getTimes.totalRequests,
         x.putTimes.totalRequests + x.delTimes.totalRequests,
         (x.getTimes.totalRequests + x.putTimes.totalRequests + x.delTimes.totalRequests)*1.0/x.runTimeMs*1000))
        .reduceLeft((x,y) => (x._1 + y._1, x._2 + y._2, x._3 + y._3, x._4 + y._4, x._5 + y._5, x._6 + y._6))
      ((n,id),
       "getl=" + (totals._1.quantile(0.99)/1000.0),
       "putl=" + (totals._2.quantile(0.99)/1000.0),
//       "std(putl)=" + stdev(putTimes).intValue + "," +
//       "std(nvputl)=" + stdev(nvPutTimes).intValue + "," +
//       "std(ops)=" + stdev(ops).intValue,
       "nvputl=" + (totals._3.quantile(0.99)/1000.0),
       "r:w=" + ((1+totals._4)/(1+totals._5)) + ":1",
       "ops/s=" + (totals._6/(data.length.doubleValue/n)).intValue,
       "runs=" + (data.length.doubleValue/n))
    }).toList.sorted
  }

  def goPessimal(implicit cluster: deploylib.mesos.Cluster, classSource: Seq[ClassSource]): Unit = {
    new Task().schedule(relativeAddress(Results.suffix))
  }

  def go(replicas: Int = 1, partitions: Int = 8, nClients: Int = 8, itemsPerMachine: Int = 100000, threadCount: Int = 32, comment: String = "")(implicit cluster: deploylib.mesos.Cluster, classSource: Seq[ClassSource]): Unit = {
    new ScaleTask(replicas=replicas, partitions=partitions, nClients=nClients, itemsPerMachine=itemsPerMachine, threadCount=threadCount, comment=comment, local=false).schedule(relativeAddress(Results.suffix))
  }
}
