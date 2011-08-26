package edu.berkeley.cs.scads.storage

import edu.berkeley.cs.scads.comm._

import org.apache.avro._
import generic._
import specific._

import org.apache.zookeeper.{ Environment => _, _ }

import net.lag.logging.Logger

import edu.berkeley.cs.avro.runtime._
import edu.berkeley.cs.avro.marker._

import scala.util.Random
import collection.mutable.ArrayBuffer

import java.io._
import java.lang.{ Integer => JInteger }

import com.sleepycat.je._


/**
 * Class for creating/accessing/managing namespaces for a set of scads storage nodes with a given zookeeper root.
 * TODO: Remove reduancy in CreateNamespace functions
 * TODO: Add ability to delete namespaces
 * TODO: Move parition management code into namespace
 */
class ScadsCluster(val root: ZooKeeperProxy#ZooKeeperNode) { self =>
  val namespaces = root.getOrCreate("namespaces")
  val clients = root.getOrCreate("clients")
  val randomGen = new Random(0)
  val clientID = clients.createChild("client", mode = CreateMode.EPHEMERAL_SEQUENTIAL).name.replaceFirst("client", "").toInt

  implicit val cluster = this

  protected val logger = Logger()

  //TODO Nice storagehandler, cluster wrap-up

  def getAvailableServers(): List[StorageService] = {
    val availableServers = root.getOrCreate("availableServers").children
    for (server <- availableServers)
      yield new StorageService().parse(server.data)
  }

	def getAvailableServers(prefix:String):List[StorageService] = {
		val availableServers = root.getOrCreate("availableServers").children.filter( _.name.split("!").head.equals(prefix))
    for (server <- availableServers)
      yield new StorageService().parse(server.data)
	}

  def getRandomServers(nbServer: Int): List[StorageService] = {
    val availableServers = root("availableServers").children.toSeq
    require(availableServers.size > 0)
    (1 to nbServer)
      .map(i => randomGen.nextInt(availableServers.size))
      .map(i => availableServers(i))
      .map(n => new StorageService().parse(n.data)).toList
  }

  def shutdown: Unit = {
    getAvailableServers.foreach(_ !! ShutdownStorageHandler())
  }

  case class UnknownNamespace(ns: String) extends Exception

  def getNamespace(ns: String,
                   keySchema: Schema,
                   valueSchema: Schema,
                   valueClass: String): GenericNamespace = {
    //val namespace = new GenericNamespace(ns, 5000, namespaces, keySchema, valueSchema)
    //namespace.loadOrCreate
    //namespace

    val namespace = new GenericNamespace(ns, self, namespaces, keySchema, valueSchema, valueClass)
    namespace.open()
    namespace
  }

  def getNamespace(ns: String): GenericNamespace = {
    val keySchema = Schema.parse(new String(namespaces(ns)("keySchema").data))
    val valueSchema = Schema.parse(new String(namespaces(ns)("valueSchema").data))
    val valueClass = new String(namespaces(ns)("valueClass").data)
    //val namespace = new GenericNamespace(ns, 5000, namespaces, keySchema, valueSchema)
    //namespace.load
    //return namespace

    getNamespace(ns, keySchema, valueSchema,valueClass)
  }

  def createNamespace(ns: String,
                      keySchema: Schema,
                      valueSchema: Schema,
                      valueClass: String,
                      servers : Seq[(Option[GenericRecord], Seq[StorageService])]): GenericNamespace = {
    //val namespace = new GenericNamespace(ns, 5000, namespaces, keySchema, valueSchema)
    //namespace.create(servers)
    //namespace

    val namespace = new GenericNamespace(ns, self, namespaces, keySchema, valueSchema, valueClass)
    namespace.create()
    namespace.setPartitionScheme(servers.map {
      // TODO: provide methods in the namespaces which understand the typed version 
      // so we don't have to use toBytes. for now, it is not that important b/c createNamespace 
      // is not called repeatedly
      case (optRec, seq) => (optRec.map(_.toBytes), seq)
    })
    namespace
  }

  // currently unused
  //def getHashNamespace[KeyType <: ScalaSpecificRecord, ValueType <: ScalaSpecificRecord](ns: String, routingFieldPos : Seq[Int])
  //    (implicit keyType: scala.reflect.Manifest[KeyType], valueType: scala.reflect.Manifest[ValueType]): SpecificHashRoutingNamespace[KeyType, ValueType] = {
  //  val namespace = new SpecificHashRoutingNamespace[KeyType, ValueType](ns, 5000, namespaces, routingFieldPos)
  //  namespace.loadOrCreate
  //  namespace
  //}

  def getNamespace[KeyType <: SpecificRecord : Manifest, ValueType <: SpecificRecord : Manifest](ns: String): SpecificNamespace[KeyType, ValueType] = {
    val namespace = new SpecificNamespace[KeyType, ValueType](ns, self, namespaces)
    namespace.open()
    namespace
  }

  def getInMemoryNamespace[KeyType <: SpecificRecord : Manifest, ValueType <: SpecificRecord : Manifest](ns: String): SpecificInMemoryNamespace[KeyType, ValueType] = {
    val namespace = new SpecificInMemoryNamespace[KeyType, ValueType](ns, self, namespaces)
    namespace.open()
    namespace
  }

  def createNamespace[KeyType <: SpecificRecord : Manifest, ValueType <: SpecificRecord : Manifest](ns: String, servers: Seq[(Option[KeyType], Seq[StorageService])]): SpecificNamespace[KeyType, ValueType] = {
    val namespace = new SpecificNamespace[KeyType, ValueType](ns, self, namespaces)
    namespace.create()
    namespace.setPartitionScheme(servers.map { case (optRec, seq) => (optRec.map(_.toBytes), seq) })
    namespace
  }

  def getNamespace[PairType <: AvroPair : Manifest](ns: String): PairNamespace[PairType] = {
    val namespace = new PairNamespace[PairType](ns, self, namespaces)
    namespace.open()
    namespace
  }

  def createNamespace[PairType <: AvroPair : Manifest](ns: String, servers: Seq[(Option[IndexedRecord], Seq[StorageService])]): PairNamespace[PairType] = {
    val namespace = new PairNamespace[PairType](ns, self, namespaces)
    namespace.create()
    namespace.setPartitionScheme(servers.map { case (optRec, seq) => (optRec.map(namespace.keyToBytes), seq) })
    namespace
  }

}

/** A ScadsCluster which manages a set of storage nodes. useful for testing
 * primarily. the managed methods are not thread-safe, for simplicity */
class ManagedScadsCluster(_root: ZooKeeperProxy#ZooKeeperNode) extends ScadsCluster(_root) {

  /** The storage nodes managed by this cluster */
  private val managedStorageNodes = new ArrayBuffer[StorageHandler]

  @inline private def toStorageService(ra: RemoteActor): StorageService = 
    StorageService(ra.host, ra.port, ra.id)

  /** Get a list of all the managed storage nodes, as services */
  def managedServices: Seq[StorageService] =
    managedStorageNodes.map(sh => toStorageService(sh.remoteHandle)).toSeq

  /** Get a list of all the managed storage nodes, as storage handlers */
  def managedNodes: Seq[StorageHandler] =
    managedStorageNodes.toSeq

  /** Shut all the managed storage nodes down */
  def shutdownCluster(): Unit = {
    managedStorageNodes foreach (_.stop)
    managedStorageNodes.clear()
  }

  def blockUntilReady(clusterSize: Int): Unit = {
    while(getAvailableServers.size < clusterSize) {
      logger.info("Waiting for cluster to start " + cluster.getAvailableServers.size + " of " + clusterSize + " ready.")
      Thread.sleep(1000)
    }
  }

  /** Ensure that this cluster is managing at least num nodes. Nodes are added
   * as necessary, but nodes will not be removed.*/
  def ensureCapacity(num: Int): Unit = {
    require(num >= 0, "num must be non-negative")
    if (num > managedStorageNodes.size) 
      managedStorageNodes ++= (1 to num - managedStorageNodes.size).map(_ => newStorageHandler())
    assert(managedStorageNodes.size >= num)
  }

  /** Ensure that this cluster is managing exactly num nodes. ensureExactly(0)
   * is equivalent to shutdownCluster() */
  def ensureExactly(num: Int): Unit = {
    require(num >= 0, "num must be non-negative")
    if (num < managedStorageNodes.size) { // remove
      val numToRemove = managedStorageNodes.size - num
      managedStorageNodes.takeRight(numToRemove).foreach(_.stop)
      managedStorageNodes.remove(managedStorageNodes.size - numToRemove, numToRemove)
    } else if (num > managedStorageNodes.size) // add
      managedStorageNodes ++= (1 to num - managedStorageNodes.size).map(_ => newStorageHandler())
    assert(managedStorageNodes.size == num)
  }

  /** Add a new node to be managed by this cluster */
  def addNode(): StorageHandler = {
    val handler = newStorageHandler()
    managedStorageNodes += handler
    handler
  }
  def addNamedNode(name:String): StorageHandler = {
    val handler = newStorageHandler(name)
    managedStorageNodes += handler
    handler
  }

  private def makeScadsTempDir() = {
    val tempDir = File.createTempFile("scads", "testdb")
    /* This strange sequence of delete and mkdir is required so that BDB can
     * acquire a lock file in the created temp directory */
    tempDir.delete()
    tempDir.mkdir()
    tempDir
  }

  private def newStorageHandler(name:Option[String] = None): StorageHandler = {
    val config = new EnvironmentConfig
    config.setConfigParam(EnvironmentConfig.LOG_MEM_ONLY, "true")
    config.setAllowCreate(true)
    config.setTransactional(true)
    config.setSharedCache(true) /* share cache w/ all other test handlers in proces */

    /** Try to never run the checkpointer */
    config.setConfigParam(EnvironmentConfig.CHECKPOINTER_BYTES_INTERVAL, JInteger.MAX_VALUE.toString)

    val dir = makeScadsTempDir()
    logger.info("Opening test BDB Environment: " + dir + ", " + config)
    val env = new Environment(dir, config)
    new StorageHandler(env, root, name)
  }

}
