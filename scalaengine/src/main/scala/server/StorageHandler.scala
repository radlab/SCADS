package edu.berkeley.cs.scads.storage

import net.lag.logging.Logger
import com.sleepycat.je.{Cursor,Database, DatabaseConfig, DatabaseException, DatabaseEntry, Environment, LockMode, OperationStatus, Durability, Transaction}

import edu.berkeley.cs.scads.comm._

import org.apache.avro.Schema
import edu.berkeley.cs.avro.runtime._

import org.apache.zookeeper.CreateMode

import java.util.Comparator
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong
import java.util.{ Arrays => JArrays }

import scala.collection.JavaConversions._
import scala.collection.mutable.{ Set => MSet, HashSet }

import org.apache.zookeeper.KeeperException.NodeExistsException

object StorageHandler {
  val idGen = new AtomicLong
}

/**
 * Basic implementation of a storage handler using BDB as a backend.
 */
class StorageHandler(env: Environment, val root: ZooKeeperProxy#ZooKeeperNode) 
  extends ServiceHandler[StorageServiceOperation] {

  val counterId = StorageHandler.idGen.getAndIncrement()

  override def toString = 
    "<CounterID: %d, EnvDir: %s, Handle: %s>".format(counterId, env.getHome.getCanonicalPath, remoteHandle)

  /** Logger must be lazy so we can reference in startup() */
  protected lazy val logger = Logger()
  implicit def toOption[A](a: A): Option[A] = Option(a)

  /** Hashmap of currently open partition handler, indexed by partitionId.
   * Must also be lazy so we can reference in startup() */
  protected lazy val partitions = new ConcurrentHashMap[String, PartitionHandler]

  /**
   * Factory for NamespaceContext
   */
  object NamespaceContext {
    def apply(schema: Schema): NamespaceContext = {
      val comparator = new AvroComparator { val keySchema = schema }
      NamespaceContext(comparator, new HashSet[(String, PartitionHandler)])
    }
  }

  /**
   * Contains metadata for namespace
   */
  case class NamespaceContext(comparator: AvroComparator, partitions: MSet[(String, PartitionHandler)])

  /** 
   * Map of namespaces to currently open partitions for that namespace.
   * The values of this map (mutable sets) must be synchronized on before reading/writing 
   */
  protected lazy val namespaces = new ConcurrentHashMap[String, NamespaceContext]

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
    makeDatabase("partitiondb", None, None)

  private def makeDatabase(databaseName: String, keySchema: Schema, txn: Option[Transaction]): Database =
    makeDatabase(databaseName, Some(new AvroBdbComparator(keySchema)), txn)

  private def makeDatabase(databaseName: String, keySchema: String, txn: Option[Transaction]): Database =
    makeDatabase(databaseName, Some(new AvroBdbComparator(keySchema)), txn)

  private def makeDatabase(databaseName: String, comparator: Option[Comparator[Array[Byte]]], txn: Option[Transaction]): Database = {
    val dbConfig = new DatabaseConfig
    dbConfig.setAllowCreate(true)
    comparator.foreach(comp => dbConfig.setBtreeComparator(comp))
    dbConfig.setTransactional(true)
    env.openDatabase(txn.orNull, databaseName, dbConfig)
  }

  private def keySchemaFor(namespace: String) = {
    val nsRoot = getNamespaceRoot(namespace)
    val keySchema = new String(nsRoot("keySchema").data)
    Schema.parse(keySchema)
  }

  /** 
   * Preconditions:
   *   (1) namespace is a valid namespace in zookeeper
   *   (2) keySchema is already set in the namespace/keySchema file
   *       in ZooKeeper 
   */
  private def makePartitionHandler(
      database: Database, namespace: String, partitionIdLock: ZooKeeperProxy#ZooKeeperNode,
      startKey: Option[Array[Byte]], endKey: Option[Array[Byte]]) =
    new PartitionHandler(database, partitionIdLock, startKey, endKey, getNamespaceRoot(namespace), keySchemaFor(namespace))

  /** Iterator scans the entire cursor and does not close it */
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
    logger.debug("Created StorageHandler" + remoteHandle.toString)
    availServs.createChild(remoteHandle.toString, remoteHandle.toBytes, CreateMode.EPHEMERAL_SEQUENTIAL)

    /* Reopen partitions */
    val cursor = partitionDb.openCursor(null, null)
    cursor.map { case (key, value) =>
      (new String(key.getData), (new CreatePartitionRequest).parse(value.getData))
    } foreach { case (partitionId, request) =>

      logger.info("Recreating partition %s from request %s".format(partitionId, request))

      /* Grab partition root from ZooKeeper */
      val partitionsDir = getNamespaceRoot(request.namespace).apply("partitions")

      /* Create the lock file, assuming that it does not already exist (since
       * the lock files are created ephemerally, so if this node dies, all the
       * lock files should die accordingly) */
      val partitionIdLock =
        try {
          partitionsDir.createChild(partitionId)
        } catch {
          case e: NodeExistsException =>
            /* The lock file has not been removed yet. Assume for now that
             * this lock file belonged to this partition to begin with, and we
             * had a race condition where the ephemeral nodes were not removed
             * in time that the node started back up. Therefore, delete the
             * existing lock file and recreate it */
            logger.critical("Clobbering lock file! Namespace: %s, PartitionID: %s".format(request.namespace, partitionId))
            partitionsDir.deleteChild(partitionId)
            partitionsDir.createChild(partitionId)
        }
      assert(partitionIdLock.name == partitionId, "Lock file was not created with the same name on restore")

      // no need to grab locks below, because startup runs w/o any invocations
      // to process (so no races can occur)

      /* Make partition handler */
      val db      = makeDatabase(request.namespace, keySchemaFor(request.namespace), None)
      val handler = makePartitionHandler(db, request.namespace, partitionIdLock, request.startKey, request.endKey)

      /* Add to our list of open partitions */
      partitions.put(partitionId, handler)

      val ctx = getOrCreateContextForNamespace(request.namespace) 
      ctx.partitions += ((partitionId, handler))

    }
    cursor.close()

  }

  /**
   * WARNING: you must synchronize on the context for a namespace before
   * performaning any mutating operations (or to have the correct memory
   * read semantics)
   *
   * TODO: in the current implementation, once a namespace lock is created, it
   * remains for the duration of the JVM process (and is thus not eligible for
   * GC). this makes implementation easier (since we don't have to worry about
   * locks changing over time), but wastes memory
   */
  private def getOrCreateContextForNamespace(namespace: String) = {
    val test = namespaces.get(namespace)
    if (test ne null)
      test
    else {
      val ctx = NamespaceContext(keySchemaFor(namespace))
      Option(namespaces.putIfAbsent(namespace, ctx)) getOrElse ctx
    }
  }

  @inline private def getContextForNamespace(namespace: String) = 
    Option(namespaces.get(namespace))

  @inline private def removeNamespaceContext(namespace: String) = 
    Option(namespaces.remove(namespace))

  /**
   * Performs the following shutdown tasks:
   *   Shutdown all active partitions
   *   Closes the bdb environment
   */
  protected def shutdown(): Unit = {
    root("availableServers").deleteChild(remoteHandle.toString)
    partitions.values.foreach(_.stop)
    partitions.clear()
    namespaces.clear()
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
        logger.info("[%s] CreatePartitionRequest for namespace %s, [%s, %s)", this, namespace, JArrays.toString(startKey.orNull), JArrays.toString(endKey.orNull))

        /* Grab root to namespace from ZooKeeper */
        val nsRoot = getNamespaceRoot(namespace)

        /* Grab a lock on the partitionId. 
         * TODO: Handle sequence wrap-around */
        val partitionIdLock = nsRoot("partitions").createChild(namespace, mode = CreateMode.EPHEMERAL_SEQUENTIAL)
        val partitionId = partitionIdLock.name

        //logger.info("Active partitions after insertion in ZooKeeper: %s".format(nsRoot("partitions").children.mkString(",")))

        logger.info("%d active partitions after insertion in ZooKeeper".format(nsRoot("partitions").children.size))

        val ctx = getOrCreateContextForNamespace(namespace) 

        /* For now, the creation of DBs under a namespace are executed
         * serially. It is assumed that a single node will not run multiple
         * storage handlers sharing the same namespaces (which allows us to
         * lock the namespace in memory. */
        // TODO: cleanup if fails
        val handler = ctx.synchronized {

          /* Start a new transaction to atomically make both the namespace DB,
          * and add an entry into the partition DB */
          val txn = env.beginTransaction(null, null)

          /* Open the namespace DB */
          val newDb = makeDatabase(namespace, keySchemaFor(namespace), Some(txn))

          /* Log to partition DB for recreation */
          partitionDb.put(txn, new DatabaseEntry(partitionId.getBytes), new DatabaseEntry(createRequest.toBytes))

          /* for now, let errors propogate up to the exception handler */
          txn.commit()

          /* Make partition handler from request */
          val handler = makePartitionHandler(newDb, namespace, partitionIdLock, startKey, endKey)

          /* Add to our list of open partitions */
          val test = partitions.put(partitionId, handler)
          assert(test eq null, "Partition ID was not unique: %s".format(partitionId))

          /* On success, add this partitionId to the ctx set */
          val succ = ctx.partitions.add((partitionId, handler))
          assert(succ, "Handler not successfully added to partitions")

          logger.info("[%s] %d active partitions after insertion on this StorageHandler".format(this, ctx.partitions.size))

          handler
        }

        logger.info("Partition %s in namespace %s created".format(partitionId, namespace))
        reply(CreatePartitionResponse( handler.remoteHandle.toPartitionService(partitionId, remoteHandle.toStorageService)) )
      }
      case DeletePartitionRequest(partitionId) => {
        logger.info("Deleting partition " + partitionId)

        /* Get the handler and shut it down */
        val handler = Option(partitions.remove(partitionId)) getOrElse {reply(InvalidPartition(partitionId)); return}

        val dbName = handler.db.getDatabaseName /* dbName is namespace */
        val dbEnv  = handler.db.getEnvironment

        //logger.info("Unregistering handler from MessageHandler for partition %s in namespace %s".format(partitionId, dbName))
        handler.stopListening

        logger.info("[%s] Deleting partition [%s, %s) for namespace %s", this, JArrays.toString(handler.startKey.orNull), JArrays.toString(handler.endKey.orNull), dbName)

        val ctx = getContextForNamespace(dbName) getOrElse {
          /**
           * Race condition - a request to delete a namespace is going on
           * right now- this error can actually be safely ignored since this
           * partition will be deleted (in response to the namespace deletion
           * request)
           */
          reply(RequestRejected("Partition will be removed by a delete namespace request currently in progress", msg)); return
        }

        ctx.synchronized {
          val succ = ctx.partitions.remove((partitionId, handler))
          assert(succ, "Handler not successfully removed from partitions")

          /* Delete from partitionDB, and (possibly) delete the database in a
          * single transaction */
          val txn = env.beginTransaction(null, null)

          /* Remove from bdb map */
          partitionDb.delete(txn, new DatabaseEntry(partitionId.getBytes))

          if (ctx.partitions.isEmpty) {
            /* Remove database from environment- this removes all the data
            * associated with the database */
            logger.info("[%s] Deleting database %s for namespace %s".format(this, dbName, dbName))
            handler.db.close() /* Close underlying DB */
            dbEnv.removeDatabase(txn, dbName)
          } else {
            logger.info("[%s] Garbage collecting inaccessible keys, since %d partitions in namespace %s remain", this, ctx.partitions.size, dbName)

            implicit def orderedByteArrayView(thiz: Array[Byte]) = new Ordered[Array[Byte]] {
              def compare(that: Array[Byte]) = ctx.comparator.compare(thiz, that) 
            }

            val thisPartition = handler
            var thisSlice = Slice(thisPartition.startKey, thisPartition.endKey)
            ctx.partitions.foreach { t =>
              val thatPartition = t._2
              val thatSlice = Slice(thatPartition.startKey, thatPartition.endKey)
              thisSlice = thisSlice.remove(thatSlice) /* Remove (from deletion) slice which cannot be deleted */
            }
            thisSlice.foreach((startKey, endKey) => {
              logger.info("++ [%s] Deleting range: [%s, %s)", this, JArrays.toString(startKey.orNull), JArrays.toString(endKey.orNull))
              handler.deleteRange(startKey, endKey, txn)
            })
          }

          txn.commit()
        }

        handler.stop

        reply(DeletePartitionResponse())
      }
      case GetPartitionsRequest() => {
        reply(GetPartitionsResponse(partitions.toList.map( a => a._2.remoteHandle.toPartitionService(a._1, remoteHandle.toStorageService))))

      }
      case DeleteNamespaceRequest(namespace) => {

        val ctx = removeNamespaceContext(namespace).getOrElse { reply(InvalidNamespace(namespace)); return }

        logger.info("WARNING: About to delete namespace %s! All information and metadata will be lost!", namespace)

        ctx.synchronized {
          val txn = env.beginTransaction(null, null)
          ctx.partitions.foreach { case (partitionId, handler) => 
            partitions.remove(partitionId) /* remove from map */
            handler.stop /* Closes DB handle */
            logger.info("Deleting metadata for partition %s", partitionId)
            partitionDb.delete(txn, new DatabaseEntry(partitionId.getBytes)) 
          }
          env.removeDatabase(txn, namespace)
          txn.commit()
        }

        reply(DeleteNamespaceResponse())
      }
      case ShutdownStorageHandler() => System.exit(0)
      case _ => reply(RequestRejected("StorageHandler can't process this message type", msg))
    }
  }
}
