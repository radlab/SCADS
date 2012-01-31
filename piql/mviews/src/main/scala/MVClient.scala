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
                         val limit: Int = 10) {
  def selectTags(tag1: String, tag2: String): Seq[String]
  def addTag(item: String, tag: String)
  def clear()

  def tpair(tag1: String, tag2: String) = {
    assert(tag1 != tag2)
    if (tag1 < tag2)
      (tag1, tag2)
    else
      (tag2, tag1)
  }

  val tags = cluster.getNamespace[Tag]("tags")
  val iindex = tags.getOrCreateIndex(
    AttributeIndex("item") :: AttributeIndex("word") :: Nil)

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

  /* TODO this really should work */
  val selectItemQueryPiql =
    tags.where("item".a === (0.?))
        .limit(limit)
        .toPiql("selectItemQuery")
  
  /* TODO should use piql */
  val selectItemQuery =
    new OptimizedQuery(
      "selectItemQuery2",
      IndexScan(
        iindex,
        ParameterValue(0) :: Nil,
        FixedLimit(10),
        true
      ),
      executor
    )

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
        .dataLimit(10) // arbitrary false promise
        .join(tags.as("t2"))
        .where("t2.word".a === (1.?))
        .where("t1.item".a === "t2.item".a)
        .limit(1024).toPiql("twoTagsPiql")

  val twoTags =
    new OptimizedQuery(
      "twoTags",
      IndexLookupJoin(
        tags,
        ParameterValue(1) :: AttributeValue(0, 1) :: Nil, // join on item field
          IndexScan(
            tags.getOrCreateIndex(
              AttributeIndex("word") :: AttributeIndex("item") :: Nil),
            (0.?) :: Nil, // filter by word === param(0)
            FixedLimit(9999),
            true)),
      executor
    )

  def selectTags(tag1: String, tag2: String) = {
    twoTags(tag1, tag2).map(
      arr => arr.head match {
        case m => m.get(1).toString
      })
  }

  def addTag(item: String, tag: String) = {
    tags.put(new Tag(tag, item))
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
  val m_tag_pairs = cluster.getNamespace[M_Tag_Pair]("m_tag_pairs")

  val selectTagPairQuery =
    m_tag_pairs.where("tag1".a === (0.?))
               .where("tag2".a === (1.?))
               .limit(limit)
               .toPiql("selectTagPairQuery")

  def selectTags(tag1: String, tag2: String) = {
    val t = tpair(tag1, tag2)
    selectTagPairQuery(t._1, t._2).map(
      arr => arr.head match {
        case m: M_Tag_Pair =>
          m.item
      })
  }

  def addTag(item: String, word: String) = {
    var mpairs = List[M_Tag_Pair]()
    for (arr <- selectItem(item)) {
      arr.head match {
        case m: GenericData$Record => 
          val t = tpair(m.get(1).toString, word)
          mpairs ::= new M_Tag_Pair(t._1, t._2, item)
      }
    }
    m_tag_pairs ++= mpairs
    tags.put(new Tag(word, item))
  }

  def clear() = {
    tags.delete()
    m_tag_pairs.delete()
    tags.open()
    m_tag_pairs.open()
  }
}
