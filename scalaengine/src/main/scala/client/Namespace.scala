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
  protected var isNewNamespace =  root.get(namespace).isEmpty
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

  /**
   * Initializes the protocol/namespace
   */
  def init() : Unit = {
    if(isNewNamespace) {
      nsRoot = root.createChild(namespace, "".getBytes, CreateMode.PERSISTENT)
      nsRoot.createChild("keySchema", getKeySchema().toString.getBytes, CreateMode.PERSISTENT)
      nsRoot.createChild("valueSchema", getValueSchema.toString.getBytes, CreateMode.PERSISTENT)
      
      logger.debug("Created Namespace" + nsRoot )
    }else{
      throw new RuntimeException("Loading not yet supported")
    }
  }
}
