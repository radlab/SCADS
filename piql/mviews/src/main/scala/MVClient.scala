package edu.berkeley.cs
package scads
package piql
package mviews

import net.lag.logging.Logger
import org.apache.avro.generic._

import opt._
import plans._
import comm._
import storage._
import storage.client.index._

/* unified interface to tag store */
abstract class TagClient(val cluster: ScadsCluster,
                         implicit val executor: QueryExecutor,
                         val limit: Int = 20) {
  def selectTags(tag1: String, tag2: String): Seq[String]
  def fastSelectTags(tag1: String, tag2: String)
  def addTag(item: String, tag: String): Tuple2[Long,Long]
  def removeTag(item: String, tag: String): Tuple2[Long,Long]
  def initBulk(itemTagPairs: Seq[Tuple2[String,String]])
  def clear()

  val tags = cluster.getNamespace[Tag]("tags")

  def tagToBytes(tag: String): Array[Byte] = {
    tags.keyToBytes(new Tag(tag, "foo"))
  }

  def all() = {
    tags.iterateOverRange(None, None).toList
  }

  def count() = {
    tags.iterateOverRange(None, None).size
  }

  val selectItemQuery =
    tags.where("item".a === (0.?))
        .limit(limit)
        .toPiql("selectItemQuery")
  
  def selectItem(item: String): List[String] = {
    var tags = List[String]()
    for (arr <- selectItemQuery(item)) {
      arr.head match {
        case m =>
          tags ::= m.get(1).toString
      }
    }
    tags
  }
}

/* uses join for tag intersection query */
class NaiveTagClient(clus: ScadsCluster, exec: QueryExecutor)
      extends TagClient(clus, exec) {

  protected val logger = Logger("edu.berkeley.cs.scads.piql.mviews.NaiveTagClient")

  val twoTagsPiql =
    tags.as("t1")
        .where("t1.word".a === (0.?))
        .dataLimit(1024) // arbitrary false promise
        .join(tags.as("t2"))
        .where("t2.word".a === (1.?))
        .where("t1.item".a === "t2.item".a)
        .limit(limit).toPiql("twoTagsPiql")

  def selectTags(tag1: String, tag2: String) = {
    twoTagsPiql(tag1, tag2).map(
      arr => arr.head match {
        case m => m.get(1).toString
      })
  }

  def fastSelectTags(tag1: String, tag2: String) = {
    twoTagsPiql(tag1, tag2)
  }

  def addTag(item: String, tag: String) = {
    val start = System.nanoTime / 1000
    tags.put(new Tag(tag, item))
    (System.nanoTime / 1000 - start, -1)
  }

  def removeTag(item: String, tag: String) = {
    val start = System.nanoTime / 1000
    tags.put(new Tag(tag, item), None)
    (System.nanoTime / 1000 - start, -1)
  }

  def initBulk(itemTagPairs: Seq[Tuple2[String,String]]) = {
    tags ++= itemTagPairs.map(t => new Tag(t._2, t._1))
  }

  def clear() = {
    tags.delete()
    tags.open()
  }
}

/* uses materialized view for tag intersection query */
class MTagClient(clus: ScadsCluster, exec: QueryExecutor)
      extends TagClient(clus, exec) {

  protected val logger = Logger("edu.berkeley.cs.scads.piql.mviews.MTagClient")

  // materialized pairs of tags, including the duplicate pair
  val mTagPairs = cluster.getNamespace[MTagPair]("mTagPairs")

  val selectTagPairQuery =
    mTagPairs.where("tag1".a === (0.?))
               .where("tag2".a === (1.?))
               .limit(limit)
               .toPiql("selectTagPairQuery")

  def selectTags(tag1: String, tag2: String) = {
    selectTagPairQuery(tag1, tag2).map(
      arr => arr.head match {
        case m: MTagPair =>
          m.item
      })
  }

  def fastSelectTags(tag1: String, tag2: String) = {
    selectTagPairQuery(tag1, tag2)
  }

  def addTag(item: String, word: String) = {
    val start = System.nanoTime / 1000
    tags.put(new Tag(word, item))
    val dt1 = System.nanoTime / 1000 - start
    val assoc = selectItem(item)
    var mpairs = List[MTagPair]()
    for (a <- assoc) {
      mpairs ::= new MTagPair(a, word, item)
      mpairs ::= new MTagPair(word, a, item)
    }
    mpairs ::= new MTagPair(word, word, item) // the duplicate pair
    mTagPairs ++= mpairs
    (dt1, System.nanoTime / 1000 - start)
  }

  def removeTag(item: String, word: String) = {
    val start = System.nanoTime / 1000
    tags.put(new Tag(word, item), None)
    val dt1 = System.nanoTime / 1000 - start
    var toDelete = List[MTagPair]()
    for (a <- selectItem(item)) {
      toDelete ::= new MTagPair(a, word, item)
      toDelete ::= new MTagPair(word, a, item)
    }
    mTagPairs --= toDelete
    (dt1, System.nanoTime / 1000 - start)
  }

  def initBulk(itemTagPairs: Seq[Tuple2[String,String]]) = {
    var allTags = List[Tag]()
    var allTagPairs = List[MTagPair]()
    itemTagPairs.groupBy(_._1).foreach {
      t =>
        val item = t._1
        var tags = t._2.map(_._2).sorted

        // queue the normal put
        tags.foreach(t => allTags ::= new Tag(t, item))

        // materialize all ordered pairs, including the duplicate pair
        for (x <- tags) {
          for (y <- tags) {
            allTagPairs ::= new MTagPair(x, y, item)
          }
        }
    }
    logger.info("Tag list size: " + allTags.length)
    logger.info("Materialized view size: " + allTagPairs.length)
    tags ++= allTags
    mTagPairs ++= allTagPairs
  }

  def clear() = {
    tags.delete()
    mTagPairs.delete()
    tags.open()
    mTagPairs.open()
  }
}
