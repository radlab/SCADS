package edu.berkeley.cs.scads.storage

import edu.berkeley.cs.scads.comm._

import org.apache.avro.Schema

import org.apache.zookeeper.CreateMode

import org.apache.avro.util.Utf8
import scala.actors._
import scala.actors.Actor._
import org.apache.log4j.Logger
import org.apache.avro.generic.GenericData.{Array => AvroArray}
import org.apache.avro.generic.{GenericData, IndexedRecord, GenericDatumWriter}
import org.apache.avro.io.{BinaryData, BinaryEncoder}

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

  def getNamespace(ns: String, keySchema: Schema, valueSchema: Schema, splitPoints: List[GenericData.Record]): GenericNamespace = {
    if(!namespaces.updateChildren(false).contains(ns)) {
      createAndConfigureNamespace(ns, keySchema, valueSchema, splitPoints)
    }
    new GenericNamespace(ns, 5000, root)
  }

  def getNamespace[KeyType <: ScalaSpecificRecord, ValueType <: ScalaSpecificRecord](ns: String, splitPoints:List[ScalaSpecificRecord] = Nil)(implicit keyType: scala.reflect.Manifest[KeyType], valueType: scala.reflect.Manifest[ValueType]): SpecificNamespace[KeyType, ValueType] = {
    if(!namespaces.updateChildren(false).contains(ns)) {
      val keySchema = keyType.erasure.newInstance.asInstanceOf[KeyType].getSchema()
      val valueSchema = valueType.erasure.newInstance.asInstanceOf[ValueType].getSchema()
      createAndConfigureNamespace(ns, keySchema, valueSchema)
    }
    return new SpecificNamespace[KeyType, ValueType](ns, 5000, root)
  }

  def createAndConfigureNamespace(ns:String, keySchema: Schema, valueSchema: Schema, splitPoints:List[IndexedRecord] = Nil): Unit = {
		val nsRoot = namespaces.createChild(ns, "".getBytes, CreateMode.PERSISTENT)
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
            Some(serializeRecord(splitPoints(i-2)))
          else
            None,
          if (i != (sps+1))
            Some(serializeRecord(splitPoints(i-1)))
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
  }

  /* TODO: library pimping scala avro object */
  protected def serializeRecord(rec: IndexedRecord): Array[Byte] = {
    val outBuffer = new java.io.ByteArrayOutputStream
    val encoder = new BinaryEncoder(outBuffer)
    val writer = new GenericDatumWriter[IndexedRecord](rec.getSchema())
    writer.write(rec, encoder)
    outBuffer.toByteArray
  }
}
