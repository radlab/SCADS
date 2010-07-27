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
import scala.util.Random

/**
 * Class for creating/accessing/managing namespaces for a set of scads storage nodes with a given zookeeper root.
 * TODO: Remove reduancy in CreateNamespace functions
 * TODO: Add ability to delete namespaces
 * TODO: Move parition management code into namespace
 */
class ScadsCluster(root: ZooKeeperProxy#ZooKeeperNode) {
  val namespaces = root.getOrCreate("namespaces")
  val randomGen = new Random(0)
  implicit val cluster = this


  def getAvailableServers(): List[StorageService] = {
    val availableServers = root("availableServers").children
    for (server <- availableServers) yield new StorageService().parse(server.data)
  }

  def getRandomServers(nbServer: Int): List[StorageService] = {
    val availableServers = root("availableServers").children
    require(availableServers.size > 0)
    var choosenServers = for (i <- 1 to nbServer) yield randomGen.nextInt(availableServers.size)
    choosenServers = choosenServers.sortBy((a) => a)
    val choosenIter = choosenServers.iterator
    val serverIter = availableServers.iterator
    var pos : Int = 0
    var result: List[StorageService] = Nil
    while(choosenIter.hasNext){
      val choosenPos : Int = choosenIter.next
      while(pos != choosenPos ){
        serverIter.next
        pos += 1
      }
      result = new StorageService().parse(serverIter.next.data) :: result
    }
    return result
  }

  case class UnknownNamespace(ns: String) extends Exception


  def getNamespace(ns: String, keySchema: Schema, valueSchema: Schema, splitPoints: List[GenericData.Record]): GenericNamespace = {
    val namespace = new GenericNamespace(ns, 5000, namespaces, keySchema, valueSchema)
    namespace.init()
    return namespace
  }

  def getNamespace[KeyType <: ScalaSpecificRecord, ValueType <: ScalaSpecificRecord](ns: String, partitions: List[(Option[KeyType], StorageService)] = Nil)(implicit keyType: scala.reflect.Manifest[KeyType], valueType: scala.reflect.Manifest[ValueType]): SpecificNamespace[KeyType, ValueType] = {
    val namespace = new SpecificNamespace[KeyType, ValueType](ns, 5000, namespaces)
    namespace.init()
    return namespace
  }


}
