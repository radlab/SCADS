package edu.berkeley.cs.scads.storage

import edu.berkeley.cs.scads.comm._
import edu.berkeley.cs.scads.storage.routing._

import org.apache.log4j.Logger
import org.apache.avro.Schema
import org.apache.avro.generic.IndexedRecord
import org.apache.zookeeper.CreateMode



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


  protected val logger: Logger

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
    logger.debug("Created Namespace" + nsRoot )
    nsRoot = root.createChild(namespace, "".getBytes, CreateMode.PERSISTENT)
    nsRoot.createChild("keySchema", getKeySchema().toString.getBytes, CreateMode.PERSISTENT)
    nsRoot.createChild("valueSchema", getValueSchema.toString.getBytes, CreateMode.PERSISTENT)
  }
}




