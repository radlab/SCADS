package edu.berkeley.cs.scads.storage

import edu.berkeley.cs.scads.comm._

import org.apache.avro.Schema

import org.apache.zookeeper.CreateMode

import org.apache.avro.util.Utf8
import scala.actors._
import scala.actors.Actor._
import org.apache.log4j.Logger
import org.apache.avro.generic.GenericData.{Array => AvroArray}
import org.apache.avro.generic.GenericData
import org.apache.avro.io.BinaryData

import scala.collection.mutable.HashMap
import java.util.Arrays
import scala.concurrent.SyncVar
import scala.tools.nsc.interpreter.AbstractFileClassLoader
import scala.tools.nsc.io.AbstractFile
import com.googlecode.avro.runtime.ScalaSpecificRecord

/**
 * Class for creating/accessing/managing namespaces for a set of scads storage nodes with a given zookeeper root.
 * TODO: Remove reduancy in CreateNamespace functions
 * TODO: Add ability to delete namespaces
 * TODO: Move parition management code into namespace
 */
class ScadsCluster(root: ZooKeeperProxy#ZooKeeperNode) {
	val namespaces = root.getOrCreate("namespaces")

  /* If namespace exists, just return false, otherwise, create namespace and return true */
  @deprecated("don't use")
  def createNamespaceIfNotExists(ns: String, keySchema: Schema, valueSchema: Schema): Boolean = {
    val nsRoot =
      try {
        namespaces.get(ns)
      } catch {
        case nse:java.util.NoSuchElementException =>
          null
        case exp =>
          throw exp
      }
    if (nsRoot == null) {
      createNamespace(ns,keySchema,valueSchema)
      true
    } else
      false
  }

  //TODO: There should perhaps be some load-balancing since we just pick a totally random server at this point
  def createNamespace[KeyType <: ScalaSpecificRecord, ValueType <: ScalaSpecificRecord](ns: String, keySchema: Schema, valueSchema: Schema)(implicit keyType: scala.reflect.Manifest[KeyType], valueType: scala.reflect.Manifest[ValueType]): Namespace[KeyType, ValueType] = {
    val available = root.get("availableServers").updateChildren(false)
    if (available.size <= 0)
      throw new Exception("No available servers")
    val serv = available.toArray.apply((scala.math.random*available.size).intValue)
    createNamespace[KeyType, ValueType](ns, keySchema, valueSchema, List[RemoteNode](new RemoteNode(serv._1,Integer.parseInt(new String(serv._2.data)))))
  }

	def createNamespace[KeyType <: ScalaSpecificRecord, ValueType <: ScalaSpecificRecord](ns: String, keySchema: Schema, valueSchema: Schema, servers: List[RemoteNode])(implicit keyType: scala.reflect.Manifest[KeyType], valueType: scala.reflect.Manifest[ValueType]): Namespace[KeyType, ValueType] = {
		val nsRoot = namespaces.createChild(ns, "".getBytes, CreateMode.PERSISTENT)
		nsRoot.createChild("keySchema", keySchema.toString.getBytes, CreateMode.PERSISTENT)
		nsRoot.createChild("valueSchema", valueSchema.toString.getBytes, CreateMode.PERSISTENT)

		val partition = nsRoot.getOrCreate("partitions/1")
		val policy = new PartitionedPolicy
		policy.partitions = List(new KeyPartition)

		partition.createChild("policy", policy.toBytes, CreateMode.PERSISTENT)
		partition.createChild("servers", "".getBytes, CreateMode.PERSISTENT)

    servers.foreach(s => {
      val cr = new ConfigureRequest
      cr.namespace = ns
      cr.partition = "1"
      Sync.makeRequest(s, ActorName("Storage"), cr)
    })

    new SpecificNamespace[KeyType, ValueType](ns, 5000, root)
	}

  def createAndConfigureNamespace[KeyType <: ScalaSpecificRecord, ValueType <: ScalaSpecificRecord](ns:String, splitPoints:List[ScalaSpecificRecord])(implicit keyType: scala.reflect.Manifest[KeyType], valueType: scala.reflect.Manifest[ValueType]): Namespace[KeyType, ValueType] = {
		val nsRoot = namespaces.createChild(ns, "".getBytes, CreateMode.PERSISTENT)
    val keySchema = keyType.erasure.newInstance.asInstanceOf[ScalaSpecificRecord].getSchema
    val valueSchema = valueType.erasure.newInstance.asInstanceOf[ScalaSpecificRecord].getSchema
		nsRoot.createChild("keySchema", keySchema.toString.getBytes, CreateMode.PERSISTENT)
		nsRoot.createChild("valueSchema", valueSchema.toString.getBytes, CreateMode.PERSISTENT)
    val sps = splitPoints.size
    val available = root.get("availableServers").updateChildren(false)
    if (available.size < sps)
      throw new Exception("Not enough available servers")
    var i = 1
    available.foreach(serv => {
      if (i <= (sps+1)) {
        val node = new RemoteNode(serv._1,Integer.parseInt(new String(serv._2.data)))
	      val partition = nsRoot.getOrCreate("partitions/"+i)
	      val policy = new PartitionedPolicy
        val kp = new KeyPartition(
          if (i != 1)
            Some(splitPoints(i-2).toBytes)
          else
            None,
          if (i != (sps+1))
            Some(splitPoints(i-1).toBytes)
          else
            None)
 	      policy.partitions = List(kp)

        println("part: "+i)
        println("pol: "+kp)

        partition.createChild("policy", policy.toBytes, CreateMode.PERSISTENT)
		    partition.createChild("servers", "".getBytes, CreateMode.PERSISTENT)
        val cr = new ConfigureRequest
        cr.namespace = ns
        cr.partition = i+""
        Sync.makeRequest(node, ActorName("Storage"), cr)
        i += 1
      }
    })
    new SpecificNamespace[KeyType, ValueType](ns, 5000, root)
  }

  @deprecated("don't use")
	def addPartition(ns:String, name:String, policy:PartitionedPolicy):Unit = {
		val partition = namespaces.getOrCreate(ns+"/partitions/"+name)

		partition.createChild("policy", policy.toBytes, CreateMode.PERSISTENT)
		partition.createChild("servers", "".getBytes, CreateMode.PERSISTENT)
	}

  @deprecated("don't use")
  def addPartition(ns:String,name:String):Unit = {
		val policy = new PartitionedPolicy
		policy.partitions = List(new KeyPartition)
		addPartition(ns,name,policy)
	}

  @deprecated("don't use")
  def getNamespace[KeyType <: ScalaSpecificRecord, ValueType <: ScalaSpecificRecord](ns: String)(implicit keyType: scala.reflect.Manifest[KeyType], valueType: scala.reflect.Manifest[ValueType]): Namespace[KeyType, ValueType] = {
    namespaces.children.get(ns) match {
      case Some(_) => new SpecificNamespace[KeyType, ValueType](ns, 5000, root)
      case None => {
        createNamespace[KeyType, ValueType](ns, keyType.erasure.newInstance.asInstanceOf[ScalaSpecificRecord].getSchema, valueType.erasure.newInstance.asInstanceOf[ScalaSpecificRecord].getSchema)
        namespaces.updateChildren(false)
        new SpecificNamespace[KeyType, ValueType](ns, 5000, root)
      }
    }
  }
}
