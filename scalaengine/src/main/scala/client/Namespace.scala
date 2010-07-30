package edu.berkeley.cs.scads.storage

import edu.berkeley.cs.scads.comm._
import edu.berkeley.cs.scads.storage.routing._

import scala.actors._
import scala.actors.Actor._
import java.util.Arrays

import scala.collection.mutable.HashMap
import scala.concurrent.SyncVar
import scala.tools.nsc.interpreter.AbstractFileClassLoader
import scala.tools.nsc.io.AbstractFile

import org.apache.log4j.Logger

import org.apache.avro.Schema
import org.apache.avro.generic.IndexedRecord
import org.apache.avro.util.Utf8
import org.apache.avro.generic.GenericData.{Array => AvroArray}
import org.apache.avro.generic.{GenericData, GenericDatumReader, GenericDatumWriter}
import org.apache.avro.io.{BinaryData, DecoderFactory, BinaryEncoder, BinaryDecoder}
import org.apache.avro.io.DecoderFactory
import com.googlecode.avro.runtime.AvroScala._
import com.googlecode.avro.runtime.ScalaSpecificRecord

import org.apache.zookeeper.CreateMode

/**
 * Implementation of Scads Namespace that returns ScalaSpecificRecords
 */
class SpecificNamespace[KeyType <: ScalaSpecificRecord, ValueType <: ScalaSpecificRecord]
    (namespace:String, timeout:Int, root: ZooKeeperProxy#ZooKeeperNode)
    (implicit  cluster : ScadsCluster, keyType: scala.reflect.Manifest[KeyType], valueType: scala.reflect.Manifest[ValueType]) 
        extends QuorumProtocol[KeyType, ValueType](namespace, timeout, root)(cluster) with RoutingProtocol[KeyType, ValueType] {
  protected val keyClass = keyType.erasure.asInstanceOf[Class[ScalaSpecificRecord]]
  protected val valueClass = valueType.erasure.asInstanceOf[Class[ScalaSpecificRecord]]
  protected val keySchema = keyType.erasure.newInstance.asInstanceOf[KeyType].getSchema()
  protected val valueSchema = valueType.erasure.newInstance.asInstanceOf[ValueType].getSchema()


  protected def serializeKey(key: KeyType): Array[Byte] = key.toBytes
  protected def serializeValue(value: ValueType): Array[Byte] = value.toBytes

  protected def getKeySchema() : Schema = keySchema

  protected def getValueSchema() : Schema = valueSchema

  protected def deserializeKey(key: Array[Byte]): KeyType = {
    val ret = keyClass.newInstance.asInstanceOf[KeyType]
    ret.parse(key)
    ret
  }

  protected def deserializeValue(value: Array[Byte]): ValueType = {
    val ret = valueClass.newInstance.asInstanceOf[ValueType]
    ret.parse(value)
    ret
  }

}

class GenericNamespace(namespace:String,
                       timeout:Int,
                       root: ZooKeeperProxy#ZooKeeperNode,
                       val keySchema:Schema,
                       val valueSchema:Schema)
                      (implicit cluster : ScadsCluster)
        extends QuorumProtocol[GenericData.Record, GenericData.Record](namespace, timeout, root)(cluster)
                with RoutingProtocol[GenericData.Record, GenericData.Record] {
  val decoderFactory = DecoderFactory.defaultFactory()
  val keyReader = new GenericDatumReader[GenericData.Record](keySchema)
  val valueReader = new GenericDatumReader[GenericData.Record](valueSchema)
  val keyWriter = new GenericDatumWriter[GenericData.Record](keySchema)
  val valueWriter = new GenericDatumWriter[GenericData.Record](valueSchema)



  protected def getKeySchema() =  keySchema
  protected def getValueSchema() = valueSchema

  protected def serializeKey(key: GenericData.Record): Array[Byte] = key.toBytes
  protected def serializeValue(value: GenericData.Record): Array[Byte] = value.toBytes

  protected def deserializeKey(key: Array[Byte]): GenericData.Record = {
    val decoder = decoderFactory.createBinaryDecoder(key, null)
    keyReader.read(null, decoder)
  }

  protected def deserializeValue(value: Array[Byte]): GenericData.Record = {
    val decoder = decoderFactory.createBinaryDecoder(value, null)
    valueReader.read(null, decoder)
  }

}

/**
 * Quorum Protocol
 */
abstract class QuorumProtocol[KeyType <: IndexedRecord, ValueType <: IndexedRecord]
      (namespace:String,
       timeout:Int,
       root: ZooKeeperProxy#ZooKeeperNode) (implicit cluster : ScadsCluster)
              extends Namespace[KeyType, ValueType](namespace, timeout, root)(cluster) {
  protected val logger = Logger.getLogger("Namespace")

  protected val rand = new scala.util.Random
  protected def pickRandomServer(key: KeyType): PartitionService = {
    val servers = serversForKey(key)
    servers(rand.nextInt(servers.size))
  }

  protected def serversForKey(key: KeyType): List[PartitionService]

  def put[K <: KeyType, V <: ValueType](key: K, value: Option[V]): Unit =
    serversForKey(key).foreach(_ !? PutRequest(serializeKey(key), value.map(serializeValue)))

  def get[K <: KeyType](key: K): Option[ValueType] =
    pickRandomServer(key) !? GetRequest(serializeKey(key)) match {
      case GetResponse(value) => value map deserializeValue
      case _ => throw new RuntimeException("Unexpected Message")
    }

  def getPrefix[K <: KeyType](key: K, prefixSize: Int, limit: Option[Int] = None, ascending: Boolean = true):Seq[(KeyType,ValueType)] = throw new RuntimeException("Unimplemented")
  def getRange(start: Option[KeyType], end: Option[KeyType], limit: Option[Int] = None, offset: Option[Int] = None, backwards:Boolean = false): Seq[(KeyType,ValueType)] = throw new RuntimeException("Unimplemented")
  def size():Int = throw new RuntimeException("Unimplemented")
  def ++=(that:Iterable[(KeyType,ValueType)]): Unit = throw new RuntimeException("Unimplemented")

}


/**
 * The new protocol interface
 * TODO Should this abstract class be renamed to protocol?
 */
abstract class Namespace[KeyType <: IndexedRecord, ValueType <: IndexedRecord]
    (val namespace:String,
     val timeout:Int,
     val root: ZooKeeperProxy#ZooKeeperNode)
    (implicit var cluster : ScadsCluster)
        extends KeyValueStore[KeyType, ValueType] {

  protected var nsRoot : ZooKeeperProxy#ZooKeeperNode = null

  protected def getKeySchema() : Schema


  protected def getValueSchema() : Schema


  protected def serializeKey(key: KeyType): Array[Byte]
  protected def serializeValue(value: ValueType): Array[Byte]
  protected def deserializeKey(key: Array[Byte]): KeyType
  protected def deserializeValue(value: Array[Byte]): ValueType

  /**
   * Returns the default replication Factor when initializing a new NS
   */
  protected def defaultReplicationFactor() : Int = 1

  def doesNamespaceExist() : Boolean = !root.get(namespace).isEmpty

  /**
   * Loads the namespace configuration. If it does not exists, it creates a new one with the min replication factor on
   * randomly selected storagehandlers
   */
  def loadOrCreate() : Unit = {
    if(doesNamespaceExist){
      load()
    }else{
      var randServers = cluster.getRandomServers(defaultReplicationFactor)
      val startKey : Option[KeyType] = None
      create(List((startKey, randServers)))
    }   
  }

  /**
   * Loads and initializes the protocol for an existing namespace
   */
  def load() : Unit = {
    require(root.get(namespace).isEmpty)
  }

  /**
   *  Creates a new NS with the given servers
   *  The ranges has the form (startKey, servers). The first Key has to be None
   */
  def create(ranges : List[(Option[KeyType], List[StorageService])]) : Unit = {
    require(root.get(namespace).isEmpty)
    require(ranges.size > 0)
    require(ranges.head._1 == None)
    println("Creating Namespace" + nsRoot )
    nsRoot = root.createChild(namespace, "".getBytes, CreateMode.PERSISTENT)
    nsRoot.createChild("keySchema", getKeySchema().toString.getBytes, CreateMode.PERSISTENT)
    nsRoot.createChild("valueSchema", getValueSchema.toString.getBytes, CreateMode.PERSISTENT)
    nsRoot.createChild("partitions", getValueSchema.toString.getBytes, CreateMode.PERSISTENT)

  }
}

