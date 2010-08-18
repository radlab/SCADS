package edu.berkeley.cs.scads.storage

import org.apache.log4j.Logger
import com.sleepycat.je.{Cursor,Database, DatabaseConfig, DatabaseException, DatabaseEntry, Environment, LockMode, OperationStatus, Durability, Transaction}

import edu.berkeley.cs.scads.comm._

import org.apache.avro.Schema
import com.googlecode.avro.runtime.AvroScala._

import org.apache.zookeeper.CreateMode

import java.util.Comparator
import java.util.concurrent.ConcurrentHashMap
import scala.collection.JavaConversions._

/**
 * Basic implementation of a storage handler using BDB as a backend.
 */
class StorageHandler(env: Environment, val root: ZooKeeperProxy#ZooKeeperNode) extends ServiceHandler[StorageServiceOperation] {
  /** Logger must be lazy so we can reference in startup() */
  protected lazy val logger = Logger.getLogger("scads.storagehandler")
  implicit def toOption[A](a: A): Option[A] = Option(a)

  /** Hashmap of currently open partition handler, indexed by partitionId.
   * Must also be lazy so we can reference in startup() */
  protected lazy val partitions = new ConcurrentHashMap[String, PartitionHandler]

  /* Register a shutdown hook for proper cleanup */
  class SDRunner(sh: ServiceHandler[_]) extends Thread {
    override def run(): Unit = {
      sh.stop
    }
  }
  java.lang.Runtime.getRuntime().addShutdownHook(new SDRunner(this))

  /** Points to the DB that can recreate partitions on restart.
   * Must also be lazy so we can reference in startup() */
  private lazy val partitionDb =
    makeDatabase("partitiondb", None)

  private def makeDatabase(databaseName: String, keySchema: Schema): Database = 
    makeDatabase(databaseName, Some(new AvroBdbComparator(keySchema)))

  private def makeDatabase(databaseName: String, keySchema: String): Database = 
    makeDatabase(databaseName, Some(new AvroBdbComparator(keySchema)))

  private def makeDatabase(databaseName: String, comparator: Option[Comparator[Array[Byte]]]): Database = {
    val dbConfig = new DatabaseConfig
    dbConfig.setAllowCreate(true)
    comparator.foreach(comp => dbConfig.setBtreeComparator(comp))
    dbConfig.setTransactional(true)
    env.openDatabase(null, databaseName, dbConfig)
  }

  private def makePartitionHandler(
      namespace: String, partitionIdLock: ZooKeeperProxy#ZooKeeperNode, 
      startKey: Option[Array[Byte]], endKey: Option[Array[Byte]]) = {
    /* Configure the new database */
    logger.info("Opening bdb table for partition in namespace: " + namespace)
    val nsRoot = getNamespaceRoot(namespace)
    val keySchema = new String(nsRoot("keySchema").data)
    val db = makeDatabase(namespace, keySchema)
    new PartitionHandler(db, partitionIdLock, startKey, endKey, nsRoot, Schema.parse(keySchema))
  }

  private implicit def cursorToIterator(cursor: Cursor): Iterator[(DatabaseEntry, DatabaseEntry)] 
    = new Iterator[(DatabaseEntry, DatabaseEntry)] {
      private var cur = getNext()
      private def getNext() = {
        val tuple = (new DatabaseEntry, new DatabaseEntry)
        if (cursor.getNext(tuple._1, tuple._2, null) == OperationStatus.SUCCESS)
          Some(tuple)
        else
          None
      }
      override def hasNext = cur.isDefined
      override def next() = cur match {
        case Some(tup) =>
          cur = getNext()
          tup
        case None =>
          throw new IllegalStateException("No more elements")
      }
    }

  /**
   * Performs the following startup tasks:
   * * Register with zookeeper as an available server
   * * Reopen any partitions.
   */
  protected def startup(): Unit = {
    /* Register with the zookeper as an available server */
    val availServs = root.getOrCreate("availableServers")
    availServs.createChild(remoteHandle.toString, remoteHandle.toBytes, CreateMode.EPHEMERAL)

    /* Reopen partitions */
    val cursor = partitionDb.openCursor(null, null)
    cursor.map { case (key, value) => 
      (new String(key.getData), (new CreatePartitionRequest).parse(value.getData)) 
    } foreach { case (partitionId, request) => 

      logger.info("Recreating partition %s from request %s".format(partitionId, request))

      /* Grab namespace root from ZooKeeper */
      val nsRoot = getNamespaceRoot(request.namespace)

      /* Grab the lock file. It should already exist, since we're recreating
       * the partition */
      val partitionIdLock = 
        nsRoot("partitions")
          .get(partitionId)
          .getOrElse(throw new IllegalStateException("Cannot find ZooKeeper Node for partition: " + partitionId)) // TODO: What do we do in this case?

      /* Make partition handler */
      val handler = makePartitionHandler(request.namespace, partitionIdLock, request.startKey, request.endKey)

      /* Add to our list of open partitions */
      partitions.put(partitionId, handler)
    }
    cursor.close()

  }

  /**
   * Performs the following shutdown tasks:
   * * TODO: Shutdown all active partitions
   */
  protected def shutdown(): Unit = {
    root("availableServers").deleteChild(remoteHandle.toString)
    partitions.values.foreach(_.stop)
    partitions.clear()
    partitionDb.close()
    env.close()
  }

  private def getNamespaceRoot(namespace: String): ZooKeeperProxy#ZooKeeperNode = 
    root("namespaces")
      .get(namespace)
      .getOrElse(throw new RuntimeException("Attempted to open namespace that doesn't exist in zookeeper: " + namespace))

  protected def process(src: Option[RemoteActorProxy], msg: StorageServiceOperation): Unit = {
    def reply(msg: MessageBody) = src.foreach(_ ! msg)

    msg match {
      case createRequest @ CreatePartitionRequest(namespace, startKey, endKey) => {
        /* Grab root to namespace from ZooKeeper */
        val nsRoot = getNamespaceRoot(namespace)

        /* Grab a lock on the partitionId */
        val partitionIdLock = nsRoot("partitions").createChild(namespace, mode = CreateMode.PERSISTENT_SEQUENTIAL)
        val partitionId = partitionIdLock.name

        logger.info("Active partitions after insertion in ZooKeeper: %s".format(nsRoot("partitions").children.mkString(",")))

        /* Make partition handler from request */
        val handler = makePartitionHandler(namespace, partitionIdLock, startKey, endKey)

        /* Add to our list of open partitions */
        partitions.put(partitionId, handler)

        /* Log to partition DB for recreation */
        partitionDb.put(null, new DatabaseEntry(partitionId.getBytes), new DatabaseEntry(createRequest.toBytes))

        logger.info("Partition " + partitionId + " created")
        reply(CreatePartitionResponse( handler.remoteHandle.toPartitionService(partitionId, remoteHandle.toStorageService)) )
      }
      case DeletePartitionRequest(partitionId) => {
        logger.info("Deleting partition " + partitionId)

        /* Get the handler and shut it down */
        val handler = Option(partitions.get(partitionId)) getOrElse {reply(InvalidPartition(partitionId)); return}
        val dbName = handler.db.getDatabaseName()
        logger.info("Stopping partition " + partitionId + " for delete")
        handler.stop

        /* Remove from in memory map */
        partitions.remove(partitionId)

        /* Remove from bdb map */
        partitionDb.delete(null, new DatabaseEntry(partitionId.getBytes))

        /* TODO: Garbage collect data / databases that are unused */
        reply(DeletePartitionResponse())
      }
      case _ => reply(RequestRejected("StorageHandler can't process this message type", msg))
    }
  }
}
