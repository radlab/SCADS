package edu.berkeley.cs.scads.storage

import edu.berkeley.cs.scads.comm._
import edu.berkeley.cs.scads.storage.routing._

import net.lag.logging.Logger
import org.apache.avro.Schema
import org.apache.avro.generic.IndexedRecord
import org.apache.zookeeper.CreateMode

import scala.collection.mutable.ListBuffer

abstract class Namespace[KeyType <: IndexedRecord,
                ValueType <: IndexedRecord, 
                RecordType <: IndexedRecord,
                RangeType]
    (val namespace: String,
     val timeout: Int,
     val root: ZooKeeperProxy#ZooKeeperNode)  (implicit val cluster : ScadsCluster)
  extends KeyValueStore[KeyType, ValueType, RecordType, RangeType] {



  protected val logger: Logger = Logger()
  protected var nsRoot: ZooKeeperProxy#ZooKeeperNode = _

  private val onLoadHandlers: ListBuffer[() => Unit] = new ListBuffer[() => Unit]
  private val onCreateHandlers: ListBuffer[Seq[(Option[KeyType], Seq[StorageService])] => Unit] = 
    new ListBuffer[Seq[(Option[KeyType], Seq[StorageService])] => Unit]
  private val onDeleteHandlers: ListBuffer[() => Unit] = new ListBuffer[() => Unit] 

  /**
   * Register an event handler to be invoked on creation
   */
  protected def onLoad(f: => Unit): Unit = {
    onLoadHandlers append (() => f)
  }

  /**
   * Register an event handler to be invoked on creation
   */
  protected def onCreate(f: Seq[(Option[KeyType], Seq[StorageService])] => Unit): Unit = {
    onCreateHandlers append f
  }

  /**
   * Register an event handler to be invoked on deletion
   */
  protected def onDelete(f: => Unit): Unit = {
    onDeleteHandlers append (() => f)
  }

  val keySchema: Schema
  val valueSchema: Schema

  /**
   * Return the key schema of this namespace
   */
  def getKeySchema() : Schema = keySchema

  /**
   * Return the value schema of this namespace
   */
  def getValueSchema() : Schema = valueSchema

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
      logger.debug("Creating nsRoot for namespace: " + namespace)
      nsRoot = root.createChild(namespace, "".getBytes, CreateMode.PERSISTENT)
      //println("nsRoot: " + nsRoot)
      //println("getKeySchema: " + getKeySchema)
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
