package edu.berkeley.cs.scads.storage

import org.apache.log4j.Logger
import com.sleepycat.je.{Cursor,Database, DatabaseConfig, DatabaseException, DatabaseEntry, Environment, LockMode, OperationStatus, Durability, Transaction}

import edu.berkeley.cs.scads.comm._

import org.apache.avro.Schema
import com.googlecode.avro.runtime.AvroScala._

import org.apache.zookeeper.CreateMode


/**
 * Basic implementation of a storage handler using BDB as a backend.
 */
class StorageHandler(env: Environment, val root: ZooKeeperProxy#ZooKeeperNode) extends ServiceHandler[StorageServiceOperation] {
  protected val logger = Logger.getLogger("scads.storagehandler")

  /* Hashmap of currently open partition handler, indexed by partitionId */
  protected var partitions = new scala.collection.immutable.HashMap[String, PartitionHandler]

  /**
   * Performs the following startup tasks:
   * * Register with zookeeper as an available server
   * * TODO: Reopen any partitions.
   */
  protected def startup(): Unit = {
    /* Register with the zookeper as an available server */
    val availServs = root.getOrCreate("availableServers")
    availServs.createChild(remoteHandle.toString, remoteHandle.toBytes, CreateMode.EPHEMERAL)
  }

  /**
   * Performs the following shutdown tasks:
   * * TODO: Shutdown all active partitions
   * * TODO: Close the BDB Environment
   */
  protected def shutdown(): Unit = {
    root("availableServers").deleteChild(remoteHandle.toString)
    MessageHandler.unregisterActor(remoteHandle)
  }

  protected def process(src: Option[RemoteActorProxy], msg: StorageServiceOperation): Unit = {
    def reply(msg: MessageBody) = src.foreach(_ ! msg)

    msg match {
      case createRequest @ CreatePartitionRequest(namespace, partitionId, startKey, endKey) => {
        /* Retrieve the KeySchema from BDB so we can setup the btree comparator correctly */
        val nsRoot = root("namespaces").get(namespace).getOrElse(throw new RuntimeException("Attempted to open namespace that doesn't exist in zookeeper: " + createRequest))
        val keySchemaJson = new String(nsRoot("keySchema").data)

        /* Configure the new database */
        logger.info("Opening bdb table for partition: " + createRequest)
        val comp = new AvroComparator(keySchemaJson)
        val dbConfig = new DatabaseConfig
        dbConfig.setAllowCreate(true)
        dbConfig.setBtreeComparator(comp)
        dbConfig.setTransactional(true)

        /* Grab a lock on the partitionId. Open the database and instanciate the handler. */
        val partitionIdLock = nsRoot("partitions").createChild(partitionId, "".getBytes, CreateMode.EPHEMERAL)
        val db = env.openDatabase(null, createRequest.toJson, dbConfig)
        val handler = new PartitionHandler(db, partitionIdLock, startKey, endKey, nsRoot)

        /* Add to our list of open partitions */
        partitions.synchronized {
          partitions += ((partitionId, handler))
        }

        reply(CreatePartitionResponse(handler.remoteHandle))
      }
      case _ => reply(ProcessingException("Unimplmented", ""))
    }
  }
}
