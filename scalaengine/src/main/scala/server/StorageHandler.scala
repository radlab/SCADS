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

        logger.info("Partition " + partitionId + " created")
        reply(CreatePartitionResponse(handler.remoteHandle.toPartitionService))
      }
      case SplitPartitionRequest(partitionId, newPartitionId, splitPoint) => {
        /* Make sure the split is valid and grab lock on newPartitionId */
        val handler = partitions.get(partitionId).getOrElse {reply(InvalidPartition(partitionId)); return}
        if(handler.startKey.map(handler.compare(_, splitPoint) < 0).getOrElse(true) &&
           handler.endKey.map(handler.compare(_, splitPoint) > 0).getOrElse(true))
         throw new RuntimeException("Invalid Split Point")
        val newPartitionIdLock = handler.nsRoot("partitions").createChild(newPartitionId, "".getBytes, CreateMode.EPHEMERAL)

        logger.info("Stopping partition " + partitionId + " for split")
        handler.stop

        /* Copy the data from the old partition to the new one */
        logger.info("Opening new partition " + newPartitionId + ".")
        val txn = env.beginTransaction(null, null)
        val comp = new AvroBdbComparator(handler.keySchema.toString)
        val dbConfig = new DatabaseConfig
        dbConfig.setAllowCreate(true)
        dbConfig.setBtreeComparator(comp)
        dbConfig.setTransactional(true)
        val p2DbName = CreatePartitionRequest(handler.nsRoot.name, newPartitionId, splitPoint, handler.endKey).toJson
        val p2Db = env.openDatabase(txn, p2DbName, dbConfig)

        logger.info("Begining copy from partition " + partitionId + " to " + newPartitionId)
        handler.iterateOverRange(splitPoint, handler.endKey, txn=txn)((key, value, cursor) => {
          logger.info(new IntRec().parse(key.getData))
          p2Db.put(txn, key, value)
          cursor.delete()
        })

        /* Update partition metadata to reflect split */
        val p1oldDbName = handler.db.getDatabaseName()
        val p1NewDbName = CreatePartitionRequest(handler.nsRoot.name, handler.partitionIdLock.name, handler.startKey, splitPoint).toJson
        handler.db.close()
        env.renameDatabase(txn, p1oldDbName, p1NewDbName)
        txn.commit()

        /* Create and return new handlers */
        logger.info("Split of " + partitionId + " complete.  Opening new partition handlers")
        val p1Db = env.openDatabase(null, p1NewDbName, dbConfig)
        val p1NewHandler = new PartitionHandler(p1Db, handler.partitionIdLock, handler.startKey, splitPoint, handler.nsRoot, handler.keySchema)
        val p2NewHandler = new PartitionHandler(p2Db, newPartitionIdLock, splitPoint, handler.endKey, handler.nsRoot, handler.keySchema)

        reply(SplitPartitionResponse(p1NewHandler.remoteHandle.toPartitionService, p2NewHandler.remoteHandle.toPartitionService))
      }
      case MergePartitionRequest(partitionId1, partitionId2) => {
        val handler1 = partitions.get(partitionId1).getOrElse {reply(InvalidPartition(partitionId1)); return}
        val handler2 = partitions.get(partitionId2).getOrElse {reply(InvalidPartition(partitionId2)); return}

        /* Validate the merge and shutdown request handling for both partitions */
        if(handler1.keySchema != handler2.keySchema) throw new RuntimeException("Can't merge handlers with different schemas")
        if(handler1.compare(handler1.endKey.get, handler2.startKey.get) != 0) throw new RuntimeException("Partition1 endKey different than Partition2 startKey. Merge Aborted. " + handler1.endKey.get.toList + " " +  handler2.startKey.get.toList)
        logger.info("Stopping partitions " + partitionId1 + " and " + partitionId2 + " for merge")
        handler1.stop
        handler2.stop

        logger.info("Begining copy of data from " + partitionId2 + " to " + partitionId1)
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
        txn.commit()

        logger.info("Merge of " + partitionId1 + " and " + partitionId2 + " complete.  Opening new partition handler")
        val newHandler = new PartitionHandler(env.openDatabase(null, newName, dbConfig), handler1.partitionIdLock, handler1.startKey, handler2.endKey, handler1.nsRoot, handler1.keySchema)

        reply(MergePartitionResponse(newHandler.remoteHandle.toPartitionService))
      }
      case DeletePartitionRequest(partitionId) => {
        /* Get the handler and shut it down */
        val handler = partitions.get(partitionId).getOrElse {reply(InvalidPartition(partitionId)); return}
        val dbName = handler.db.getDatabaseName()
        logger.info("Stopping partition " + partitionId + " for delete")
        handler.stop
        handler.db.close()

        logger.info("Deleting partition " + partitionId)
        env.removeDatabase(null, dbName)
        reply(DeletePartitionResponse())
      }
      case _ => reply(RequestRejected("StorageHandler can't process this message type", msg))
    }
  }
}
