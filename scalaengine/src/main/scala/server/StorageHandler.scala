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

import com.googlecode.avro.runtime.ScalaSpecificRecord


class StorageHandler(env: Environment, root: ZooKeeperProxy#ZooKeeperNode) extends ServiceHandler {
  /* Hashmap and case class to hold relevent data about open namespaces */
  protected case class Namespace(db: Database, keySchema: Schema, comp: AvroComparator)
  protected var namespaces: Map[String, Namespace] = new scala.collection.immutable.HashMap[String, Namespace]

  /* Threadpool for execution of incoming requests */
  protected val outstandingRequests = new ArrayBlockingQueue[Runnable](1024)
  protected val executor = new ThreadPoolExecutor(5, 20, 30, TimeUnit.SECONDS, outstandingRequests)

  protected val logger = Logger.getLogger("scads.storagehandler")
  protected val remoteHandle = MessageHandler.registerService(this)

  /* Register a shutdown hook for proper cleanup */
  class SDRunner(sh: StorageHandler) extends Thread {
    override def run(): Unit = {
      sh.shutdown()
    }
  }
  java.lang.Runtime.getRuntime().addShutdownHook(new SDRunner(this))
  startup()

  private def startup(): Unit = {
    /* Register with the zookeper as an available server */
    val availServs = root.getOrCreate("availableServers")
    availServs.createChild(remoteHandle.toString, remoteHandle.toBytes, CreateMode.EPHEMERAL)

    /* TODO: Open already created namespaces? */
  }

  /* Implicit conversions */
  implicit def toOption[A](a: A): Option[A] = Option(a)
  implicit def toDbe(buff: Array[Byte]): DatabaseEntry = new DatabaseEntry(buff)
  implicit def toByteArray(dbe: DatabaseEntry): Array[Byte] = {
    if(dbe.getOffset == 0)
      return dbe.getData
    else
      throw new RuntimeException("Unimplemented")
  }

  private def decodeKey(ns:Namespace, dbe:DatabaseEntry): GenericData.Record = {
    val decoder = DecoderFactory.defaultFactory().createBinaryDecoder(dbe.getData(), dbe.getOffset(), dbe.getSize, null)
    val reader = new GenericDatumReader[GenericData.Record](ns.keySchema)
    reader.read(null,decoder)
  }

  private def mkSchema(old:Schema,newsize:Int):Schema = {
    val result = Schema.createRecord(old.getName, old.getDoc, old.getNamespace, old.isError);
    val fields = old.getFields
    val newfields = new java.util.ArrayList[Schema.Field](newsize-1)
    for (i <- (0 to (newsize-1))) {
      val of = fields.get(i)
      val nf = new Schema.Field(of.name,of.schema,of.doc,of.defaultValue,of.order)
      newfields.add(nf)
    }
    result.setFields(newfields)
    result
  }

  private class ShippedClassLoader(ba:Array[Byte],targetClass:String) extends ClassLoader {
    override def findClass(name:String):Class[_] = {
      if (name.equals(targetClass))
        defineClass(name, ba, 0, ba.size)
      else
        Class.forName(name)
    }
  }

  private def deserialize(name:String, ba:Array[Byte]):Any = {
    try {
      val loader = new ShippedClassLoader(ba,name)
      Class.forName(name,false,loader)
    } catch {
      case ex:java.io.IOException => {
        ex.printStackTrace
        (null,0)
      }
    }
  }

  class Request(src: RemoteNode, req: Message) extends Runnable {
    def reply(body: MessageBody) = {
      throw new RuntimeException("Unimplemented")
    }

    val process: PartialFunction[Object, Unit] = {
      case _ => throw new RuntimeException("Unimplemented")
    }

    def run():Unit = {
      try {
        //println(req)
        process(req.body)
      }
      catch {
        case e: Throwable => {
          logger.error("ProcessingException", e)
          e.printStackTrace
          var cause = e.getCause
          while (cause != null) {
            e.printStackTrace
            cause = e.getCause
          }
          val resp = new ProcessingException
          resp.cause = e.toString()
          resp.stacktrace = e.getStackTrace().mkString("\n")
          reply(resp)
        }
      }
    }
  }

  def openNamespace(ns: String, partition: String): Unit = {
    namespaces.synchronized {
      if (!namespaces.contains(ns)) {
        val nsRoot = root("namespaces").get(ns) match {
          case Some(nsr) => nsr
          case None => throw new RuntimeException("Attempted to open namespace that doesn't exist in zookeeper: " + ns)
        }
        val keySchema = new String(nsRoot("keySchema").data)

        val comp = new AvroComparator(keySchema)
        val dbConfig = new DatabaseConfig
        dbConfig.setAllowCreate(true)
        dbConfig.setBtreeComparator(comp)
        dbConfig.setTransactional(true)

        namespaces += ((ns, Namespace(env.openDatabase(null, ns, dbConfig), Schema.parse(keySchema), comp)))
        logger.info("namespace " + ns + " opened")

        nsRoot("partitions").apply(partition).getOrCreate("servers").createChild(remoteHandle.toString, remoteHandle.toBytes, CreateMode.EPHEMERAL)
      }
    }
  }

  def shutdown(): Unit = {
    var ok = true
    namespaces.synchronized {
      namespaces.foreach { tuple:(String,Namespace) => {
        val (nsStr, ns) = tuple
        logger.info("Closing: "+nsStr)
        try {
          ns.db.close()
        } catch {
          case dbe:DatabaseException => {
            ok = false
            logger.error("Could not close namespace "+nsStr+": "+dbe)
          }
          case excp => {
            ok = false
            throw excp
          }
        }
        logger.info(if(ok) "[OK]" else "[FAIL]")
      } }
    }
    logger.info("Closing environment")
    try {
      env.close()
    } catch {
      case dbe:DatabaseException => {
        ok = false
        logger.error("Could not close environment: "+dbe)
      }
      case excp => {
        ok = false
        throw excp
      }
    }
    logger.info(if(ok) "[OK]" else "[FAIL]")
  }

  def receiveMessage(src: Option[RemoteActor], msg:MessageBody): Unit = {
    try {
      //executor.execute(new Request(src, msg))
    } catch {
      case ree: java.util.concurrent.RejectedExecutionException => {
        val resp = new ProcessingException
        resp.cause = "Thread pool exhausted"
        resp.stacktrace = ree.toString
        //replyWithError(src,msg,resp)
      }
      case e: Throwable => {
        logger.error("ProcessingException", e)
        e.printStackTrace
        var cause = e.getCause
        while (cause != null) {
          e.printStackTrace
          cause = e.getCause
        }
        val resp = new ProcessingException
        resp.cause = e.toString()
        resp.stacktrace = e.getStackTrace().mkString("\n")
        //replyWithError(src,msg,resp)
      }
    }
  }
}
