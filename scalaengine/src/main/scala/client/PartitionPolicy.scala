package edu.berkeley.cs.scads.storage

import edu.berkeley.cs.scads.comm._

import scala.actors._
import scala.actors.Actor._
import java.util.Arrays

import scala.collection.mutable.HashMap
import scala.concurrent.SyncVar
import scala.tools.nsc.interpreter.AbstractFileClassLoader
import scala.tools.nsc.io.AbstractFile

import org.apache.log4j.Logger

import org.apache.avro.Schema
import org.apache.avro.util.Utf8
import org.apache.avro.generic.GenericData.{Array => AvroArray}
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.IndexedRecord
import org.apache.avro.io.BinaryData

import org.apache.zookeeper.CreateMode


abstract trait PartitionPolicy[KeyType <: IndexedRecord] {
  protected val logger: Logger
  protected val schema: Schema
  protected val keyClass: Class[_]
  protected val nsNode: ZooKeeperProxy#ZooKeeperNode
  private var nodeCache:Array[polServer] = null

  /* DeSerialization Methods */
  protected def serializeKey(key: KeyType): Array[Byte]
  protected def deserializeKey(key: Array[Byte]): KeyType

  class RangeIterator(start:Int, end:Int) extends Iterator[polServer] {
    private var cur = start
    def hasNext():Boolean = {
      cur <= end
    }

    def next():polServer = {
      if (cur > end)
        throw new NoSuchElementException()
      cur += 1
      nodeCache((cur-1))
    }
  }

  case class polServer(min: Option[Array[Byte]],max: Option[Array[Byte]],nodes:List[RemoteNode]) extends Comparable[polServer] {
    def compareTo(p:polServer):Int = {
      (max, p.max) match {
        case (None, None) => 0
        case (None, _) => 1
        case (_, None) => -1
        case (Some(a), Some(b)) => org.apache.avro.io.BinaryData.compare(a, 0, b, 0, schema)
      }
    }

    override def toString():String = {
      val sb = new java.lang.StringBuffer
      sb.append("[")
      sb.append(min.map(deserializeKey).getOrElse("-inf"))
      sb.append(", ")
      sb.append(max.map(deserializeKey).getOrElse("+inf"))
      sb.append("): ")
      nodes.foreach((node) => {
        sb.append(node)
        sb.append(" ")
      })
      return sb.toString
    }
  }

  
  def keyComp(part: Option[Array[Byte]], key: Option[Array[Byte]]): Int = {
    (part, key) match {
      case (None, None) => 0
      case (None, _) => -1
      case (_, None) => 1
      case (Some(a), Some(b)) => org.apache.avro.io.BinaryData.compare(a, 0, b, 0, schema)
    }
  }

  private def updateNodeCache():Unit = {
    val partitions = nsNode.get("partitions").updateChildren(false)
    var ranges:Int = 0
    partitions.map(part=>{
      val policyData = 	nsNode.get("partitions/"+part._1+"/policy").updateData(false)
      val policy = new PartitionedPolicy
      policy.parse(policyData)
      ranges += policy.partitions.size.toInt
    })
    nodeCache = new Array[polServer](ranges)
    var idx = 0
    partitions.map(part=>{
      val policyData = 	nsNode.get("partitions/"+part._1+"/policy").updateData(false)
      val policy = new PartitionedPolicy
      policy.parse(policyData)
		  val iter = policy.partitions.iterator
      nsNode.get("partitions/"+part._1).updateChildren(false)
      val nodes = nsNode.get("partitions/"+part._1+"/servers").updateChildren(false).toList.map(ent=>{
        new RemoteNode(ent._1,Integer.parseInt(new String(ent._2.data)))
      })
		  while (iter.hasNext) {
			  val part = iter.next
        nodeCache(idx) = new polServer(part.minKey,part.maxKey,nodes)
        idx += 1
      }
		})
    Arrays.sort(nodeCache,null)
  }

  def clearCache():Unit = {
    nodeCache = null
  }

  def printCache():Unit = {
    if (nodeCache == null)
      updateNodeCache
    println("Current cache:")
    for (i <- (0 to (nodeCache.length - 1))) {
      println(nodeCache(i))
    }
  }

  def idxForKey(key:KeyType):Int = {
    if (nodeCache == null)
      updateNodeCache
    val polKey = new polServer(None,Some(serializeKey(key)),Nil)
    val bpos = Arrays.binarySearch(nodeCache,polKey,null)
    if (bpos < 0)
      ((bpos+1) * -1)
    else if (bpos == nodeCache.length)
      bpos
    else
      bpos + 1
  }

  protected def serversForKey(key:KeyType):List[RemoteNode] = {
    val idx = idxForKey(key)
    // validate that we don't have a gap
    if (keyComp(nodeCache(idx).min, Some(serializeKey(key))) > 0) {
      logger.warn("Possible gap in partitions, returning empty server list")
      Nil
    } else
      nodeCache(idx).nodes
  }

  protected def splitRange(startKey: Option[KeyType],endKey: Option[KeyType]):RangeIterator = {
    if (nodeCache == null)
      updateNodeCache

    val sidx = startKey.map(idxForKey).getOrElse(0)
    val eidx = endKey.map(idxForKey).getOrElse(nodeCache.length - 1)

    new RangeIterator(sidx,eidx)
  }

	private def keyInPolicy(policy:Array[Byte], key: KeyType):Boolean = {
		val pdata = new PartitionedPolicy
		pdata.parse(policy)
		val iter = pdata.partitions.iterator
    val kdata = serializeKey(key)
		while (iter.hasNext) {
			val part = iter.next
      if ( (part.minKey == null ||
            keyComp(part.minKey,Some(kdata)) >= 0) &&
           (part.maxKey == null ||
            keyComp(part.maxKey,Some(kdata)) < 0) )
        return true
		}
    false
	}

  private def serversForKeySlow(key:KeyType):List[RemoteNode] = {
    val partitions = nsNode.get("partitions").updateChildren(false)
    partitions.map(part=>{
      val policyData = 	nsNode.get("partitions/"+part._1+"/policy").updateData(false)
      if (keyInPolicy(policyData,key)) {
        nsNode.get("partitions/"+part._1+"/servers").updateChildren(false).toList.map(ent=>{
          new RemoteNode(ent._1,Integer.parseInt(new String(ent._2.data)))
        })
      } else
        Nil
		}).toList.flatten(n=>n).distinct
  }
}
