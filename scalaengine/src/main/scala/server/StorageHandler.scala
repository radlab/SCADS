package edu.berkeley.cs.scads.storage

import java.util.concurrent.{BlockingQueue, ArrayBlockingQueue, ThreadPoolExecutor, TimeUnit}
import java.nio.ByteBuffer
import java.io.ByteArrayInputStream
import java.net.InetAddress

import scala.actors._
import scala.actors.Actor._
import scala.collection.mutable.ArrayBuffer

import org.apache.log4j.Logger
import com.sleepycat.je.{Cursor,Database, DatabaseConfig, DatabaseException, DatabaseEntry, Environment, LockMode, OperationStatus, Durability, Transaction}

import edu.berkeley.cs.scads.comm._
import edu.berkeley.cs.scads.comm.Conversions._

import org.apache.avro.generic.GenericData.{Array => AvroArray}
import org.apache.avro.Schema
import org.apache.avro.util.Utf8
import org.apache.avro.generic.{GenericDatumReader,GenericData}
import org.apache.avro.io.BinaryDecoder
import org.apache.avro.io.DecoderFactory

import org.apache.zookeeper.CreateMode
import com.googlecode.avro.runtime.AvroScala._
import com.googlecode.avro.runtime.ScalaSpecificRecord

/**
 * Basic implementation of a storage handler using BDB as a backend.
 */
class StorageHandler(env: Environment, val root: ZooKeeperProxy#ZooKeeperNode) extends ServiceHandler {
  implicit val remoteHandle = MessageHandler.registerService(this).toStorageService
  protected val logger = Logger.getLogger("scads.storagehandler")

  /* Threadpool for execution of incoming requests */
  protected val outstandingRequests = new ArrayBlockingQueue[Runnable](1024)
  protected val executor = new ThreadPoolExecutor(5, 20, 30, TimeUnit.SECONDS, outstandingRequests)

  /* Hashmap of currently open partition handler, indexed by partitionId */
  protected var partitions = new scala.collection.immutable.HashMap[String, PartitionHandler]

  /* Register a shutdown hook for proper cleanup */
  class SDRunner(sh: StorageHandler) extends Thread {
    override def run(): Unit = {
      sh.shutdown()
    }
  }
  java.lang.Runtime.getRuntime().addShutdownHook(new SDRunner(this))
  startup()

  /**
   * Performs the following startup tasks:
   * * Register with zookeeper as an available server
   * * TODO: Reopen any partitions.
   */
  private def startup(): Unit = {
    /* Register with the zookeper as an available server */
    val availServs = root.getOrCreate("availableServers")
    availServs.createChild(remoteHandle.toString, remoteHandle.toBytes, CreateMode.EPHEMERAL)
  }

  /**
   * Performs the following shutdown tasks:
   * * TODO: Shutdown all active partitions
   * * TODO: Close the BDB Environment
   */
  def shutdown(): Unit = {

  }

  /* Request handler class to be executed on this StorageHandlers threadpool */
  class Request(src: Option[RemoteActor], req: MessageBody) extends Runnable {
    def reply(msg: MessageBody) = src.foreach(_ ! msg)

    val process: PartialFunction[Object, Unit] = {
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
      case _ => throw new RuntimeException("Unimplemented")
    }

    def run():Unit = {
      try process(req) catch {
        case e: Throwable => {
          /* Get the stack trace */
          val stackTrace = e.getStackTrace().mkString("\n")
          /* Log and report the error */
          logger.error("Exception processing storage request: " + e)
          logger.error(stackTrace)
          src.foreach(_ ! ProcessingException(e.toString, stackTrace))
        }
      }
    }
  }

  /* Enque a recieve message on the threadpool executor */
  def receiveMessage(src: Option[RemoteActor], msg:MessageBody): Unit = {
    try executor.execute(new Request(src, msg)) catch {
      case ree: java.util.concurrent.RejectedExecutionException => src.foreach(_ ! RequestRejected("Thread Pool Full", msg))
      case e: Throwable => {
        /* Get the stack trace */
        var stackTrace = e.getStackTrace().mkString("\n")
        /* Log and report the error */
        logger.error("Exception enquing storage request for execution: " + e)
        logger.error(stackTrace)
        src.foreach(_ ! ProcessingException(e.toString(), stackTrace))
      }
    }
  }
}
