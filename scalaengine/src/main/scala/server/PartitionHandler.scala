package edu.berkeley.cs.scads.storage

import com.sleepycat.je.{Cursor,Database, DatabaseConfig, DatabaseException, DatabaseEntry, Environment, LockMode, OperationStatus, Durability, Transaction}
import org.apache.log4j.Logger

import edu.berkeley.cs.scads.comm._

class PartitionHandler(val db: Database, partitionIdLock: ZooKeeperProxy#ZooKeeperNode, val startKey: Option[Array[Byte]], val endKey: Option[Array[Byte]], nsRoot: ZooKeeperProxy#ZooKeeperNode) extends ServiceHandler[PartitionServiceOperation] {
  protected val logger = Logger.getLogger("scads.partitionhandler")

  protected def startup(): Unit = null
  protected def shutdown(): Unit = db.close()

  protected def process(src: Option[RemoteActorProxy], msg: PartitionServiceOperation): Unit = {
    def reply(msg: MessageBody) = src.foreach(_ ! msg)

    msg match {
      case GetRequest(key) => {
        val (dbeKey, dbeValue) = (new DatabaseEntry(key), new DatabaseEntry)
        db.get(null, dbeKey, dbeValue, LockMode.READ_COMMITTED)
        reply(GetResponse(Option(dbeValue.getData())))
      }
      case PutRequest(key, value) => {
        value match {
          case Some(v) => db.put(null, new DatabaseEntry(key), new DatabaseEntry(v))
          case None => db.delete(null, new DatabaseEntry(key))
        }
        reply(PutResponse())
      }
      case _ => src.foreach(_ ! ProcessingException("Not Implemented", ""))
    }
  }
}
