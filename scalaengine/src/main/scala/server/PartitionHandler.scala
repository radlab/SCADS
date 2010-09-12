package edu.berkeley.cs.scads.storage

import com.sleepycat.je.{Cursor,Database, DatabaseConfig, DatabaseException, DatabaseEntry, Environment, LockMode, OperationStatus, Durability, Transaction}
import org.apache.avro.Schema
import net.lag.logging.Logger

import edu.berkeley.cs.scads.comm._

import java.util.{ Arrays => JArrays }

/**
 * Handles a partition from [startKey, endKey). Refuses to service any
 * requests which fall out of this range, by returning a ProcessingException
 */
class PartitionHandler(val db: Database, val partitionIdLock: ZooKeeperProxy#ZooKeeperNode, val startKey: Option[Array[Byte]], val endKey: Option[Array[Byte]], val nsRoot: ZooKeeperProxy#ZooKeeperNode, val keySchema: Schema) extends ServiceHandler[PartitionServiceOperation] with AvroComparator {
  protected val logger = Logger()
  implicit def toOption[A](a: A): Option[A] = Option(a)

  protected def startup() { /* No-op */ }
  protected def shutdown() {
    partitionIdLock.delete()
    db.close()
  }

  override def toString = 
    "<PartitionHandler namespace: %s, keyRange: [%s, %s)>".format(
      partitionIdLock.name, JArrays.toString(startKey.orNull), JArrays.toString(endKey.orNull))

  /**
   * True iff a particular (non-null) key is bounded by [startKey, endKey)
   */
  @inline private def isInRange(key: Array[Byte]) =
    startKey.map(sk => compare(sk, key) <= 0).getOrElse(true) &&
    endKey.map(ek => compare(ek, key) > 0).getOrElse(true)

  /**
   * True iff startKey <= key
   */
  @inline private def isStartKeyLEQ(key: Option[Array[Byte]]) = 
    startKey.map(sk => /* If we have startKey that is not -INF */
        key.map(usrKey => /* If we have a user key that is not -INF */
          compare(sk, usrKey) <= 0) /* Both keys exist (not -INF), use compare to check range */
        .getOrElse(false)) /* startKey not -INF, but user key is -INF, so false */
    .getOrElse(true) /* startKey is -INF, so any user key works */

  /**
   * True iff endKey >= key
   */
  @inline private def isEndKeyGEQ(key: Option[Array[Byte]]) =
    endKey.map(ek => /* If we have endKey that is not +INF */
        key.map(usrKey => /* If we have a user key that is not +INF */
          compare(ek, usrKey) >= 0) /* Both keys exist (not +INF), use compare to check range */
        .getOrElse(false)) /* endKey not +INF, but user key is +INF, so false */
    .getOrElse(true) /* startKey is +INF, so any user key works */

  protected def process(src: Option[RemoteActorProxy], msg: PartitionServiceOperation): Unit = {
    def reply(msg: MessageBody) = src.foreach(_ ! msg)

    // key validation
    val (keysInRange, keysInQuestion) = msg match {
      /* GetRequest key must be bounded by [startKey, endKey) */
      case GetRequest(key) => (isInRange(key), Left(key))
      /* PutRequest key must be bounded by [startKey, endKey) */
      case PutRequest(key, _) => (isInRange(key), Left(key))
      /* Requires startKey <= minKey and endKey >= maxKey (so specifying the
       * entire range is allowed */
      case GetRangeRequest(minKey, maxKey, _, _, _) =>
        (isStartKeyLEQ(minKey) && isEndKeyGEQ(maxKey), Right((minKey, maxKey)))
      /* Requires startKey <= minKey and endKey >= maxKey (so specifying the
       * entire range is allowed */
      case CountRangeRequest(minKey, maxKey) =>
        (isStartKeyLEQ(minKey) && isEndKeyGEQ(maxKey), Right((minKey, maxKey)))
      /* TestSetRequest key must be bounded by [startKey, endKey) */
      case TestSetRequest(key, _, _) => (isInRange(key), Left(key))
      case _ => (true, null)
    }

    if (keysInRange) {
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
        case BulkPutRequest(records) => {
          val txn = db.getEnvironment.beginTransaction(null, null)
          records.foreach(rec => db.put(txn, new DatabaseEntry(rec.key), new DatabaseEntry(rec.value.get)))
          try {
            txn.commit()
            reply(BulkPutResponse())
          } catch {
            case e: Exception =>
              logger.error(e, "Could not commit BulkPutRequest")
              reply(ProcessingException(e.getMessage, e.getStackTrace.mkString("\n")))
          }
        }
        case GetRangeRequest(minKey, maxKey, limit, offset, ascending) => {
          logger.debug("[%s] GetRangeRequest: [%s, %s)", this, JArrays.toString(minKey.orNull), JArrays.toString(maxKey.orNull))
          val maxIsEnd = JArrays.equals(endKey.orNull, maxKey.orNull)
          val records = new scala.collection.mutable.ListBuffer[Record]
          iterateOverRange(minKey, maxKey, limit, offset, ascending, !maxIsEnd)((key, value, _) => {
            records += Record(key.getData, value.getData)
          })
          reply(GetRangeResponse(records.toList))
        }
        case CountRangeRequest(minKey, maxKey) => {
          val maxIsEnd = JArrays.equals(endKey.orNull, maxKey.orNull)
          var count = 0
          iterateOverRange(minKey, maxKey, includeMaxKey = !maxIsEnd)((_,_,_) => count += 1)
          reply(CountRangeResponse(count))
        }
        case TestSetRequest(key, value, expectedValue) => {
          val txn = db.getEnvironment.beginTransaction(null, null)
          val dbeKey = new DatabaseEntry(key)
          val dbeCurrentValue = new DatabaseEntry
          db.get(txn, dbeKey, dbeCurrentValue, LockMode.READ_COMMITTED)
          if(JArrays.equals(expectedValue.orNull, dbeCurrentValue.getData)){
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
          val iter = new PartitionIterator(src, startKey, endKey)

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
      val errorMsg = keysInQuestion match {
        case Left(key) =>
          "Expected a key in range [%s, %s), but got %s".format(
            JArrays.toString(startKey.orNull), 
            JArrays.toString(endKey.orNull), 
            JArrays.toString(key))
        case Right((minKey, maxKey)) =>
          "Expected a range bounded (inclusively) by [%s, %s), but got [%s, %s)".format(
            JArrays.toString(startKey.orNull),
            JArrays.toString(endKey.orNull),
            JArrays.toString(minKey.orNull),
            JArrays.toString(maxKey.orNull))
      }
      logger.error("Received errorneous request %s. Error was: %s", msg, errorMsg)
      reply(RequestRejected("Key(s) are out of range: %s".format(errorMsg), msg))
    }
  }

  /**
   * Delete [startKey, endKey)
   */
  def deleteEntireRange(txn: Option[Transaction]) { deleteRange(startKey, endKey, txn) }

  /**
   * Delete [lowerKey, upperKey)
   */
  def deleteRange(lowerKey: Option[Array[Byte]], upperKey: Option[Array[Byte]], txn: Option[Transaction]) {
    assert(isStartKeyLEQ(lowerKey) && isEndKeyGEQ(upperKey), "startKey <= lowerKey && endKey >= upperKey required")
    iterateOverRange(lowerKey, upperKey, includeMaxKey = false, txn = txn)((_, _, cursor) => {
      cursor.delete()
    })
  }

  /**
   * Low level method to iterate over a given range on the database. it is up
   * to the caller to validate minKey and maxKey.
   */
  private def iterateOverRange(minKey: Option[Array[Byte]], 
                               maxKey: Option[Array[Byte]], 
                               limit: Option[Int] = None, 
                               offset: Option[Int] = None, 
                               ascending: Boolean = true, 
                               includeMaxKey: Boolean = true,
                               txn: Option[Transaction] = None)
      (func: (DatabaseEntry, DatabaseEntry, Cursor) => Unit): Unit = {
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
    if (status != OperationStatus.SUCCESS) {
      cur.close()
      return
    }

    if(ascending) {
      while(toSkip > 0 && status == OperationStatus.SUCCESS) {
        status = cur.getNext(dbeKey, dbeValue, null)
        toSkip -= 1
      }

      status = cur.getCurrent(dbeKey, dbeValue, null)
      while(status == OperationStatus.SUCCESS &&
            limit.map(_ > returnedCount).getOrElse(true) &&
            maxKey.map(mk => if (includeMaxKey) compare(dbeKey.getData, mk) <= 0 else compare(dbeKey.getData, mk) < 0).getOrElse(true)) {
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
