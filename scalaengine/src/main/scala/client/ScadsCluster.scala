package edu.berkeley.cs.scads.storage

import edu.berkeley.cs.scads.comm._

import org.apache.avro.Schema

import org.apache.zookeeper.CreateMode

import org.apache.avro.util.Utf8
import scala.actors._
import scala.actors.Actor._
import net.lag.logging.Logger
import org.apache.avro.generic.GenericData.{Array => AvroArray}
import org.apache.avro.generic.{GenericData, IndexedRecord, GenericDatumWriter}
import org.apache.avro.io.{BinaryData, BinaryEncoder}

import scala.collection.mutable.HashMap
import java.util.Arrays
import scala.concurrent.SyncVar

import com.googlecode.avro.runtime._
import scala.util.Random

/**
 * Class for creating/accessing/managing namespaces for a set of scads storage nodes with a given zookeeper root.
 * TODO: Remove reduancy in CreateNamespace functions
 * TODO: Add ability to delete namespaces
 * TODO: Move parition management code into namespace
 */
class ScadsCluster(val root: ZooKeeperProxy#ZooKeeperNode) {
  val namespaces = root.getOrCreate("namespaces")
  val clients = root.getOrCreate("clients")
  val randomGen = new Random(0)
  val clientID = clients.createChild("client", mode = CreateMode.EPHEMERAL_SEQUENTIAL).name.replaceFirst("client", "").toInt

  implicit val cluster = this

  //TODO Nice storagehandler, cluster wrap-up

  def getAvailableServers(): List[StorageService] = {
    val availableServers = root.getOrCreate("availableServers").children
    for (server <- availableServers)
      yield new StorageService().parse(server.data)
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


  def getNamespace(ns: String,
                   keySchema: Schema,
                   valueSchema: Schema): GenericNamespace = {
    val namespace = new GenericNamespace(ns, 5000, namespaces, keySchema, valueSchema)
    namespace.loadOrCreate
    return namespace
  }

   def createNamespace(ns: String,
                    keySchema:
                    Schema,
                    valueSchema: Schema,
                    servers : List[(Option[GenericData.Record], List[StorageService])]): GenericNamespace = {
    val namespace = new GenericNamespace(ns, 5000, namespaces, keySchema, valueSchema)
    namespace.create(servers)
    return namespace
  }


  def getNamespace[KeyType <: ScalaSpecificRecord, ValueType <: ScalaSpecificRecord](ns: String)
      (implicit keyType: scala.reflect.Manifest[KeyType], valueType: scala.reflect.Manifest[ValueType]): SpecificNamespace[KeyType, ValueType] = {
    val namespace = new SpecificNamespace[KeyType, ValueType](ns, 5000, namespaces)
    namespace.loadOrCreate
    return namespace
  }

  def createNamespace[KeyType <: ScalaSpecificRecord, ValueType <: ScalaSpecificRecord](ns: String, servers: List[(Option[KeyType], List[StorageService])])
      (implicit keyType: scala.reflect.Manifest[KeyType], valueType: scala.reflect.Manifest[ValueType]): SpecificNamespace[KeyType, ValueType] = {
    val namespace = new SpecificNamespace[KeyType, ValueType](ns, 5000, namespaces)
    namespace.create(servers)
    return namespace
  }
}
