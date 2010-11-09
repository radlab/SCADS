package edu.berkeley.cs.scads.storage

import edu.berkeley.cs.scads.comm._
import edu.berkeley.cs.scads.storage.routing._

import net.lag.logging.Logger
import org.apache.avro.Schema
import org.apache.avro.generic.IndexedRecord
import org.apache.zookeeper.CreateMode


trait Namespace[KeyType <: IndexedRecord, ValueType <: IndexedRecord]
  extends KeyValueStore[KeyType, ValueType] {

  val namespace: String
  val timeout: Int
  val root: ZooKeeperProxy#ZooKeeperNode
  val cluster: ScadsCluster

  protected val logger: Logger = Logger()
  protected var nsRoot: ZooKeeperProxy#ZooKeeperNode = _

  private var onLoadHandlers: List[() => Unit] = Nil
  private var onCreateHandlers: List[Seq[(Option[KeyType], Seq[StorageService])] => Unit] = Nil
  private var onDeleteHandlers: List[() => Unit] = Nil

  /**
   * Register an event handler to be invoked on creation
   */
  protected def onLoad(f: => Unit): Unit = {
    onLoadHandlers ::= (() => f)
  }

  /**
   * Register an event handler to be invoked on creation
   */
  protected def onCreate(f: Seq[(Option[KeyType], Seq[StorageService])] => Unit): Unit = {
    onCreateHandlers ::= f
  }

  /**
   * Register an event handler to be invoked on deletion
   */
  protected def onDelete(f: => Unit): Unit = {
    onDeleteHandlers ::= (() => f)
  }

  /**
   * Return the key schema of this namespace
   */
  def getKeySchema() : Schema

  /**
   * Return the value schema of this namespace
   */
  def getValueSchema() : Schema

  //protected def createRecord(value : ValueType) : Array[Byte]
  //protected def extractValueFromRecord(record: Option[Array[Byte]]): Option[ValueType]
  //protected def getMetaData(record : Option[Array[Byte]]) : String
  //protected def compareRecord(data1 : Option[Array[Byte]], data2 : Option[Array[Byte]]) : Int
  //protected def compareRecord(data1 : Array[Byte], data2 : Array[Byte]) : Int


  //protected def serializeKey(key: KeyType): Array[Byte]
  //protected def serializeValue(value: ValueType): Array[Byte]
  //protected def deserializeKey(key: Array[Byte]): KeyType
  //protected def deserializeValue(value: Array[Byte]): ValueType

  //protected def serversForKey(key: KeyType): Seq[PartitionService]

  //protected def newKeyInstance: KeyType
  //protected def newRecordInstance(schema: Schema): IndexedRecord

  ///**
  // * Returns a list of ranges where to find the data for the given range.
  // *
  // * For example, for the routing table [None -> (S1,S3); 100 -> (S2, S4), 150 -> (S6, S7)]
  // * serversForRange(10,130) would return ((10, 100) -> (S1, S3); (100, 130) -> (S2, S4)) 
  // */
  //protected def serversForRange(startKey: Option[KeyType], endKey: Option[KeyType]): Seq[FullRange]

  //case class FullRange(startKey: Option[KeyType], endKey: Option[KeyType], values : Seq[PartitionService])

  /**
   * Returns the default replication Factor when initializing a new NS
   */
  protected def defaultReplicationFactor(): Int = 1

  def doesNamespaceExist(): Boolean = !root.get(namespace).isEmpty

  /** 
   * Loads the namespace configuration. If it does not exists, it creates a
   * new one with the min replication factor on randomly selected
   * storagehandlers
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

  onLoad {
    val node = root.get(namespace)
    require(node.isDefined)
    nsRoot = node.get
  }

  /**
   * Loads and initializes the protocol for an existing namespace
   */
  def load(): Unit = {
    onLoadHandlers.foreach(f => f())
  }

  onCreate { 
    ranges => {
      if(!root.get(namespace).isEmpty)
        throw new RuntimeException("Illegal namespace creation. Namespace already exists")
      if( ranges.size == 0 || ranges.head._1 != None)
        throw new RuntimeException("Illegal namespace creation - range size hast to be > 0 and the first key has to be None")
      logger.debug("Created Namespace" + nsRoot )
      nsRoot = root.createChild(namespace, "".getBytes, CreateMode.PERSISTENT)
      nsRoot.createChild("keySchema", getKeySchema.toString.getBytes, CreateMode.PERSISTENT)
      nsRoot.createChild("valueSchema", getValueSchema.toString.getBytes, CreateMode.PERSISTENT)
    }
  }

  /**
   *  Creates a new NS with the given servers
   *  The ranges has the form (startKey, servers). The first Key has to be None
   */
  def create(ranges: Seq[(Option[KeyType], Seq[StorageService])]): Unit = {
    onCreateHandlers.foreach(f => f(ranges))
  }

  onDelete {
    logger.info("Deleting zookeeper metadata for namespace %s", namespace)
    nsRoot.deleteRecursive
    nsRoot = null
  }

  /**
   * Deletes the entire namespace, along with any associated metadata. After
   * calling delete, this namespace instance is no longer usable
   */
  def delete(): Unit = {
    onDeleteHandlers.foreach(f => f())
  }

}
