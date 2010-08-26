package edu.berkeley.cs.scads.storage

import com.sleepycat.je.{Cursor,Database, DatabaseConfig, DatabaseException, DatabaseEntry, Environment, LockMode, OperationStatus, Durability, Transaction}
import org.apache.avro.Schema
import org.apache.log4j.Logger

import edu.berkeley.cs.scads.comm._

class PartitionHandler(val db: Database, val partitionIdLock: ZooKeeperProxy#ZooKeeperNode, val startKey: Option[Array[Byte]], val endKey: Option[Array[Byte]], val nsRoot: ZooKeeperProxy#ZooKeeperNode, val keySchema: Schema) extends ServiceHandler[PartitionServiceOperation] with AvroComparator {
  protected val logger = Logger.getLogger("scads.partitionhandler")
  implicit def toOption[A](a: A): Option[A] = Option(a)

  protected def startup() { /* No-op */ }
  protected def shutdown() {
    partitionIdLock.delete()
    db.close()
  }

  private def isInRange(key: Array[Byte]) =
    startKey.map(sk => compare(sk, key) <= 0).getOrElse(true) &&
    endKey.map(ek => compare(ek, key) >= 0).getOrElse(true)

  protected def process(src: Option[RemoteActorProxy], msg: PartitionServiceOperation): Unit = {
    def reply(msg: MessageBody) = src.foreach(_ ! msg)

    // key validation
    val keysToValidate = msg match {
      case GetRequest(key) => List(key)
      case PutRequest(key, _) => List(key)
      case GetRangeRequest(minKey, maxKey, _, _, _) =>
        minKey.toList ::: maxKey.toList
      case CountRangeRequest(minKey, maxKey) =>
        minKey.toList ::: maxKey.toList
      case TestSetRequest(key, _, _) => List(key)
      case _ => Nil
    }

    val outOfRangeKeys = keysToValidate filter { key => !isInRange(key) }
    if (outOfRangeKeys.isEmpty) {
      /** Invariant: All keys as input from client are valid for this
       * partition */
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
        case GetRangeRequest(minKey, maxKey, limit, offset, ascending) => {
          val records = new scala.collection.mutable.ArrayBuffer[Record]
          iterateOverRange(minKey, maxKey, limit, offset, ascending)((key, value, _) => {
            records += Record(key.getData, value.getData)
          })
          reply(GetRangeResponse(records.toList))
        }
        case CountRangeRequest(minKey, maxKey) => {
          var count = 0
          iterateOverRange(minKey, maxKey)((_,_,_) => count += 1)
          reply(CountRangeResponse(count))
        }
        case TestSetRequest(key, value, expectedValue) => {
          val txn = db.getEnvironment.beginTransaction(null, null)
          val dbeKey = new DatabaseEntry(key)
          val dbeCurrentValue = new DatabaseEntry
          db.get(txn, dbeKey, dbeCurrentValue, LockMode.READ_COMMITTED)
          if(java.util.Arrays.equals(expectedValue.orNull, dbeCurrentValue.getData)){
            value match {
              case Some(v) => db.put(txn, dbeKey, new DatabaseEntry(v))
              case None => db.delete(txn, dbeKey)
            }
            txn.commit()
            reply(TestSetResponse(true))
          } else {
            txn.abort()
            reply(TestSetResponse(false))
          }
        }
        case CopyDataRequest(src, overwrite) => {
          val txn = db.getEnvironment.beginTransaction(null, null)
          val dbeExistingValue = new DatabaseEntry
          val dbeKey = new DatabaseEntry
          val dbeValue = new DatabaseEntry
          logger.debug("Opening iterator for data copy")
          val iter = new PartitionIterator(src, None, None)

          logger.debug("Begining copy")
          iter.foreach(rec => {
            dbeKey.setData(rec.key); dbeValue.setData(rec.value.get)
            if(overwrite == true) {
              db.put(txn, dbeKey, dbeValue)
            }
            else {
              if(db.get(txn, dbeKey, dbeExistingValue, LockMode.READ_COMMITTED) != OperationStatus.SUCCESS)
                db.put(txn, dbeKey, dbeValue)
            }
          })
          logger.debug("Copy complete.  Begining commit")
          txn.commit()
          logger.debug("Comit complete")
          reply(CopyDataResponse())
        }
        case GetResponsibilityRequest() => {
          reply(GetResponsibilityResponse(startKey, endKey))
        }
        case _ => src.foreach(_ ! ProcessingException("Not Implemented", ""))
      }
    } else {
      reply(RequestRejected("The following keys are out of range: %s".format(outOfRangeKeys.mkString(",")), msg))
    }
  }

  def deleteEntireRange(txn: Option[Transaction]) {
    iterateOverRange(startKey, endKey, txn = txn)((_, _, cursor) => {
      cursor.delete()
    })
  }

  // TODO: should iterateOverRange be private? does it have to validate
  // {min,max}Key?
  def iterateOverRange(minKey: Option[Array[Byte]], maxKey: Option[Array[Byte]], limit: Option[Int] = None, offset: Option[Int] = None, ascending: Boolean = true, txn: Option[Transaction] = None)(func: (DatabaseEntry, DatabaseEntry, Cursor) => Unit): Unit = {
    val (dbeKey, dbeValue) = (new DatabaseEntry, new DatabaseEntry)
    val cur = db.openCursor(txn.orNull,null)

    var status: OperationStatus = (ascending, minKey, maxKey) match {
      case (true, None, _) => cur.getFirst(dbeKey, dbeValue, null)
      case (true, Some(startKey), _) => cur.getSearchKeyRange(new DatabaseEntry(startKey), dbeValue, null)
      case (false, _, None) => cur.getLast(dbeKey, dbeValue, null)
      case (false, _, Some(startKey)) => {
        // Check if maxKey is past the last key in the database, if so start from the end
        if(cur.getSearchKeyRange(new DatabaseEntry(startKey), dbeValue, null) == OperationStatus.NOTFOUND)
          cur.getLast(dbeKey, dbeValue, null)
        else
          OperationStatus.SUCCESS
      }
    }

    var toSkip = offset.getOrElse(0)
    var returnedCount = 0
    if (status != OperationStatus.SUCCESS) return

    if(ascending) {
      while(toSkip > 0 && status == OperationStatus.SUCCESS) {
        status = cur.getNext(dbeKey, dbeValue, null)
        toSkip -= 1
      }

      status = cur.getCurrent(dbeKey, dbeValue, null)
      while(status == OperationStatus.SUCCESS &&
            limit.map(_ > returnedCount).getOrElse(true) &&
            maxKey.map(compare(dbeKey.getData, _) <= 0).getOrElse(true)) {
        func(dbeKey, dbeValue, cur)
        returnedCount += 1
        status = cur.getNext(dbeKey, dbeValue, null)
      }
    }
    else {
      while(toSkip > 0 && status == OperationStatus.SUCCESS) {
        status = cur.getPrev(dbeKey, dbeValue, null)
        toSkip -= 1
      }

      status = cur.getCurrent(dbeKey, dbeValue, null)
      while(status == OperationStatus.SUCCESS &&
            limit.map(_ > returnedCount).getOrElse(true) &&
            minKey.map(compare(_, dbeKey.getData) <= 0).getOrElse(true)) {
        func(dbeKey, dbeValue,cur)
        returnedCount += 1
        status = cur.getPrev(dbeKey, dbeValue, null)
      }
    }
    cur.close()
  }
}
