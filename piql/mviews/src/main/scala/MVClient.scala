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
  def addTag(item: String, tag: String)
  def initBulk(itemTagPairs: Seq[Tuple2[String,String]])
  def clear()

  def tpair(tag1: String, tag2: String) = {
    if (tag1 < tag2)
      (tag1, tag2)
    else
      (tag2, tag1)
  }

  val tags = cluster.getNamespace[Tag]("tags")

  def all() = {
    tags.iterateOverRange(None, None).toList
  }

  def count() = {
    tags.iterateOverRange(None, None).size
  }

  val selectTagQuery =
    tags.where("word".a === (0.?))
        .limit(limit)
        .toPiql("selectTagQuery")

  val selectItemQuery =
    tags.where("item".a === (0.?))
        .limit(limit)
        .toPiql("selectItemQuery")
  
  def selectTag(tag: String) = {
    selectTagQuery(tag)
  }

  def selectItem(item: String) = {
    selectItemQuery(item)
  }
}

/* uses join for tag intersection query */
class NaiveTagClient(val clus: ScadsCluster, val exec: QueryExecutor)
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

  def addTag(item: String, tag: String) = {
    tags.put(new Tag(tag, item))
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
class MTagClient(val clus: ScadsCluster, val exec: QueryExecutor)
      extends TagClient(clus, exec) {

  protected val logger = Logger("edu.berkeley.cs.scads.piql.mviews.MTagClient")
  val mTagPairs = cluster.getNamespace[MTagPair]("mTagPairs")

  val selectTagPairQuery =
    mTagPairs.where("tag1".a === (0.?))
               .where("tag2".a === (1.?))
               .limit(limit)
               .toPiql("selectTagPairQuery")

  def selectTags(tag1: String, tag2: String) = {
    val t = tpair(tag1, tag2)
    selectTagPairQuery(t._1, t._2).map(
      arr => arr.head match {
        case m: MTagPair =>
          m.item
      })
  }

  def addTag(item: String, word: String) = {
    var mpairs = List[MTagPair]()
    for (arr <- selectItem(item)) {
      arr.head match {
        case m: GenericData$Record => 
          val t = tpair(m.get(1).toString, word)
          mpairs ::= new MTagPair(t._1, t._2, item)
      }
    }
    mTagPairs ++= mpairs
    tags.put(new Tag(word, item))
  }

  def initBulk(itemTagPairs: Seq[Tuple2[String,String]]) = {
    var allTags = List[Tag]()
    var allTagPairs = List[MTagPair]()
    itemTagPairs.groupBy(_._1).foreach {
      t =>
        val item = t._1
        var tags = t._2.map(_._2).sorted

        tags.foreach(t => allTags ::= new Tag(t, item))

        // materialize all unique ordered pairs
        while (tags.length > 1) {
          val head = tags.head
          tags = tags.tail
          for (y <- tags) {
            assert (head < y)
            allTagPairs ::= new MTagPair(head, y, item)
          }
        }
    }
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
