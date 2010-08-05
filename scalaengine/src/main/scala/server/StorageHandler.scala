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
  implicit def toOption[A](a: A): Option[A] = Option(a)

  /* Hashmap of currently open partition handler, indexed by partitionId */
  protected var partitions = new scala.collection.immutable.HashMap[String, PartitionHandler]

  /* Register a shutdown hook for proper cleanup */
  class SDRunner(sh: ServiceHandler[_]) extends Thread {
    override def run(): Unit = {
      sh.stop
    }
  }
  java.lang.Runtime.getRuntime().addShutdownHook(new SDRunner(this))

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
   */
  protected def shutdown(): Unit = {
    root("availableServers").deleteChild(remoteHandle.toString)
    partitions.values.foreach(_.stop)
    env.close()
  }

  protected def process(src: Option[RemoteActorProxy], msg: StorageServiceOperation): Unit = {
    def reply(msg: MessageBody) = src.foreach(_ ! msg)

    msg match {
      case createRequest @ CreatePartitionRequest(namespace, startKey, endKey) => {
        /* Retrieve the KeySchema from BDB so we can setup the btree comparator correctly */
        val nsRoot = root("namespaces").get(namespace).getOrElse(throw new RuntimeException("Attempted to open namespace that doesn't exist in zookeeper: " + createRequest))
        val keySchemaJson = new String(nsRoot("keySchema").data)

        /* Configure the new database */
        logger.info("Opening bdb table for partition: " + createRequest)
        val comp = new AvroBdbComparator(keySchemaJson)
        val dbConfig = new DatabaseConfig
        dbConfig.setAllowCreate(true)
        dbConfig.setBtreeComparator(comp)
        dbConfig.setTransactional(true)

        /* Grab a lock on the partitionId. Open the database and instanciate the handler. */
        val partitionIdLock = nsRoot("partitions").createChild(namespace, mode = CreateMode.EPHEMERAL_SEQUENTIAL)
        val partitionId = partitionIdLock.name
        val db = env.openDatabase(null, namespace, dbConfig)
        val handler = new PartitionHandler(db, partitionIdLock, startKey, endKey, nsRoot, Schema.parse(keySchemaJson))

        /* Add to our list of open partitions */
        partitions.synchronized {
          partitions += ((partitionId, handler))
        }

        logger.info("Partition " + partitionId + " created")
        reply(CreatePartitionResponse(partitionId, handler.remoteHandle.toPartitionService))
      }
      case DeletePartitionRequest(partitionId) => {
        /* Get the handler and shut it down */
        val handler = partitions.get(partitionId).getOrElse {reply(InvalidPartition(partitionId)); return}
        val dbName = handler.db.getDatabaseName()
        logger.info("Stopping partition " + partitionId + " for delete")
        handler.stop

        synchronized {
          partitions -= partitionId
        }

        logger.info("Deleting partition " + partitionId)
        /* TODO: Garbage collect data / databases that are unused */
        reply(DeletePartitionResponse())
      }
      case _ => reply(RequestRejected("StorageHandler can't process this message type", msg))
    }
  }
}
