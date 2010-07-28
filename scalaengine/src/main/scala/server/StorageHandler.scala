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
        val comp = new AvroBdbComparator(keySchemaJson)
        val dbConfig = new DatabaseConfig
        dbConfig.setAllowCreate(true)
        dbConfig.setBtreeComparator(comp)
        dbConfig.setTransactional(true)

        /* Grab a lock on the partitionId. Open the database and instanciate the handler. */
        val partitionIdLock = nsRoot("partitions").createChild(partitionId, "".getBytes, CreateMode.EPHEMERAL)
        val db = env.openDatabase(null, createRequest.toJson, dbConfig)
        val handler = new PartitionHandler(db, partitionIdLock, startKey, endKey, nsRoot, Schema.parse(keySchemaJson))

        /* Add to our list of open partitions */
        partitions.synchronized {
          partitions += ((partitionId, handler))
        }

        reply(CreatePartitionResponse(handler.remoteHandle.toPartitionService))
      }
      case SplitPartitionRequest(partitionId, splitPoint) => {
        val handler = partitions.get(partitionId).getOrElse {reply(InvalidPartition(partitionId)); return}
        throw new RuntimeException("Unimplemented")
      }
      case MergePartitionRequest(partitionId1, partitionId2) => {
        val handler1 = partitions.get(partitionId1).getOrElse {reply(InvalidPartition(partitionId1)); return}
        val handler2 = partitions.get(partitionId2).getOrElse {reply(InvalidPartition(partitionId2)); return}

        /* Validate the merge and shutdown request handling for both partitions */
        if(handler1.keySchema != handler2.keySchema) throw new RuntimeException("Can't merge handlers with different schemas")
        if(handler1.compare(handler1.endKey.get, handler2.startKey.get) != 0) throw new RuntimeException("Partition1 endKey different than Partition2 startKey. Merge Aborted. " + handler1.endKey.get.toList + " " +  handler2.startKey.get.toList)
        handler1.stop
        handler2.stop

        /* Copy the data from partition2 to partition1 */
        val txn = env.beginTransaction(null, null)
        handler2.iterateOverRange(None, None, txn=txn)((key, value, cursor) => {
          handler1.db.put(txn, key, value)
          cursor.delete()
        })

        /* Delete the old database and update the name of the remaining one to reflect the new responsibility policy */
        val oldName = handler1.db.getDatabaseName
        val newName = CreatePartitionRequest(handler1.nsRoot.name, handler1.partitionIdLock.name, handler1.startKey, handler2.endKey).toJson
        val deleteDbName = handler2.db.getDatabaseName()
        handler1.db.close()
        handler2.db.close()
        env.renameDatabase(txn, oldName, newName)
        env.removeDatabase(txn, deleteDbName)

        /* Reopen the database with the new configuration and create a new partition handler */
        val comp = new AvroBdbComparator(handler1.keySchema.toString)
        val dbConfig = new DatabaseConfig
        dbConfig.setAllowCreate(true)
        dbConfig.setBtreeComparator(comp)
        dbConfig.setTransactional(true)
        val newHandler = new PartitionHandler(env.openDatabase(txn, newName, dbConfig), handler1.partitionIdLock, handler1.startKey, handler2.endKey, handler1.nsRoot, handler1.keySchema)

        txn.commit()
        reply(MergePartitionResponse(newHandler.remoteHandle.toPartitionService))
      }
      case DeletePartitionRequest(partitionId) => {
        /* Get the handler and shut it down */
        val handler = partitions.get(partitionId).getOrElse {reply(InvalidPartition(partitionId)); return}
        val dbName = handler.db.getDatabaseName()
        handler.stop
        handler.db.close()

        env.removeDatabase(null, dbName)
        reply(DeletePartitionResponse())
      }
      case _ => reply(RequestRejected("StorageHandler can't process this message type", msg))
    }
  }
}
