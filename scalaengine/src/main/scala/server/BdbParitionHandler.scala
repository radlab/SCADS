package edu.berkeley.cs.scads.storage

import com.sleepycat.je.{Cursor,Database, DatabaseConfig, CursorConfig, DatabaseException, DatabaseEntry, Environment, LockMode, OperationStatus, Durability, Transaction}
import org.apache.avro.Schema
import net.lag.logging.Logger

import edu.berkeley.cs.scads.comm._
import edu.berkeley.cs.scads.config._
import edu.berkeley.cs.scads.util._

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer
import org.apache.avro.generic._

import scala.util.control.Breaks

import java.util.{ Arrays => JArrays }
import java.util.concurrent.{ Future => JFuture, _ }
import java.lang.reflect.Method
import java.io.{BufferedReader,ObjectInputStream,InputStreamReader,ByteArrayInputStream}
import atomic._

import edu.berkeley.cs.avro.runtime.ScalaSpecificRecord



/**
 * Handles a partition from [startKey, endKey). Refuses to service any
 * requests which fall out of this range, by returning a ProcessingException
 */
class BdbStorageManager(val db: Database, 
                        val partitionIdLock: ZooKeeperProxy#ZooKeeperNode, 
                        val startKey: Option[Array[Byte]], 
                        val endKey: Option[Array[Byte]], 
                        val nsRoot: ZooKeeperProxy#ZooKeeperNode, 
                        val keySchema: Schema, valueSchema: Schema) 
                       extends StorageManager
                       with    AvroComparator {
  protected val logger = Logger()
  protected val config = Config.config

  protected lazy val copyIteratorCtor = 
    config.getString("scads.storage.copy.iteratorType").flatMap(tpe => tpe.toLowerCase match {
      case "cursor" => Some((ps: PartitionService, minKey: Option[Array[Byte]], maxKey: Option[Array[Byte]]) =>
        new CursorBasedPartitionIterator(ps, minKey, maxKey, copyRecsPerMessage))
      case "actorfree" => Some((ps: PartitionService, minKey: Option[Array[Byte]], maxKey: Option[Array[Byte]]) =>
        new ActorlessPartitionIterator(ps, minKey, maxKey, copyRecsPerMessage))
      case invalid => 
        logger.info("copy iterator type %s is invalid", invalid)
        None
    }).getOrElse({
        logger.info("Using standard ActorlessPartitionIterator")
        (ps: PartitionService, minKey: Option[Array[Byte]], maxKey: Option[Array[Byte]]) =>
          new ActorlessPartitionIterator(ps, minKey, maxKey, copyRecsPerMessage)
    })

  protected lazy val copyRecsPerMessage = config.getInt("scads.storage.copy.recsPerMessage", 8192)

  implicit def toOption[A](a: A): Option[A] = Option(a)

  protected val cursorTimeout = 3 * 60 * 1000 // 3 minutes (in ms)

  case class CursorContext(cursorId: Int, cursor: Cursor, txn: Transaction) {
    private var lastActivity = System.currentTimeMillis
    private var valid = true
    def updateActivity(): Unit =
      lastActivity = System.currentTimeMillis
    def markInvalid(): Unit = 
      valid = false
    def isTimedout: Boolean = (lastActivity + cursorTimeout) - System.currentTimeMillis < 0
    def isValid: Boolean = valid
  }

  private val cursorIdGen = new AtomicInteger
  private val openCursors = new ConcurrentHashMap[Int, CursorContext]
  private val cursorTimeoutThread = Executors.newSingleThreadExecutor

  protected object CursorTimeoutRunnable {
    val runnable = new Runnable {
      def run(): Unit = 
        try {
          while (true) {
            Thread.sleep(cursorTimeout) // runs every 3 min
            val toRemove = openCursors.values.flatMap {
              case ctx @ CursorContext(id, cursor, txn) =>
                ctx.synchronized {
                  if (ctx.isValid) {
                    if (ctx.isTimedout) {
                      cursor.close()
                      txn.commit()
                      logger.info("Cursor %d closed and marked invalid by cleanup thread", id)
                      ctx.markInvalid()
                      List(id)
                    } else Nil
                  } else List(id)
                }
            }
            toRemove.foreach(openCursors.remove(_))
          }
        } catch {
          case e: InterruptedException =>
            logger.info("Cursor timeout thread got signal to shutdown")
        }
    }
    cursorTimeoutThread.execute(runnable)
  }

  def startup() { /* No-op */ }
  def shutdown() {
    // stop the cursor tasks
    cursorTimeoutThread.shutdownNow()
    partitionIdLock.delete()
    openCursors.values.foreach { 
      case ctx @ CursorContext(id, cursor, txn) =>
        ctx.synchronized {
          if (ctx.isValid) {
            cursor.close()
            txn.commit()
            logger.info("Cursor %d closed and marked invalid by shutdown task", id)
            ctx.markInvalid()
          }
        }
    }
    openCursors.clear()
    db.close()
  }

  private val iterateRangeBreakable = new Breaks

  override def toString = 
    "<PartitionHandler namespace: %s, keyRange: [%s, %s)>".format(
      partitionIdLock.name, JArrays.toString(startKey.orNull), JArrays.toString(endKey.orNull))

  /**
   * True iff a particular (non-null) key is bounded by [startKey, endKey)
   */
  @inline private def isInRange(key: Array[Byte]) = true //just for the moment because of the Hash partitioning
  //TODO: I turned of the check, for the hash partitiong. Though, this is really dangerous and can cause hard to find bugs.
  //We should make the starte aware of the routing, and fix it as soon as possible.
  //  startKey.map(sk => compare(sk, key) <= 0).getOrElse(true) &&
  //  endKey.map(ek => compare(ek, key) > 0).getOrElse(true)

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

                        

    // key validation
    // val (keysInRange, keysInQuestion) = msg match {
    //   /* GetRequest key must be bounded by [startKey, endKey) */
    //   case GetRequest(key) => (isInRange(key), Left(key))
    //   /* PutRequest key must be bounded by [startKey, endKey) */
    //   case PutRequest(key, _) => (isInRange(key), Left(key))
    //   /* Requires startKey <= minKey and endKey >= maxKey (so specifying the
    //    * entire range is allowed */
    //   case GetRangeRequest(minKey, maxKey, _, _, _) =>
    //     (isStartKeyLEQ(minKey) && isEndKeyGEQ(maxKey), Right((minKey, maxKey)))
    //   /* Requires startKey <= minKey and endKey >= maxKey (so specifying the
    //    * entire range is allowed */
    //   case CountRangeRequest(minKey, maxKey) =>
    //     (isStartKeyLEQ(minKey) && isEndKeyGEQ(maxKey), Right((minKey, maxKey)))
    //   /* TestSetRequest key must be bounded by [startKey, endKey) */
    //   case TestSetRequest(key, _, _) => (isInRange(key), Left(key))
    //   case _ => (true, null)
    // }

      // val errorMsg = keysInQuestion match {
      //   case Left(key) =>
      //     "Expected a key in range [%s, %s), but got %s".format(
      //       JArrays.toString(startKey.orNull), 
      //       JArrays.toString(endKey.orNull), 
      //       JArrays.toString(key))
      //   case Right((minKey, maxKey)) =>
      //     "Expected a range bounded (inclusively) by [%s, %s), but got [%s, %s)".format(
      //       JArrays.toString(startKey.orNull),
      //       JArrays.toString(endKey.orNull),
      //       JArrays.toString(minKey.orNull),
      //       JArrays.toString(maxKey.orNull))
      // }
      // logger.error("Received errorneous request %s. Error was: %s", msg, errorMsg)
      // reply(RequestRejected("Key(s) are out of range: %s".format(errorMsg), msg))


   def get(key:Array[Byte]):Option[Array[Byte]] = {
     val (dbeKey, dbeValue) = (new DatabaseEntry(key), new DatabaseEntry)
     logger.debug("Running get %s %s", dbeKey, dbeValue)
     db.get(null, dbeKey, dbeValue, LockMode.READ_COMMITTED)
     logger.debug("get operation complete %s %s", dbeKey, dbeValue)
     Option(dbeValue.getData())
   }


  def put(key:Array[Byte],value:Option[Array[Byte]]):Unit = {
    value match {
      case Some(v) => db.put(null, new DatabaseEntry(key), new DatabaseEntry(v))
      case None => db.delete(null, new DatabaseEntry(key))
    }
  }

  def testAndSet(key:Array[Byte], value:Option[Array[Byte]], expectedValue:Option[Array[Byte]]):Boolean = {
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
      true
    } else {
      txn.abort()
      false
    }
  }

  def bulkUrlPut(parser:RecParser, locations:Seq[String]):Unit = {
    val txn = db.getEnvironment.beginTransaction(null,null)
    locations foreach(location => {
      parser.setLocation(location)
      val url = new java.net.URL(location)
      parser.setInput(url.openStream)
      var kv = parser.getNext()
      while (kv != null) {
        db.put(txn, new DatabaseEntry(kv._1.asInstanceOf[Array[Byte]]), new DatabaseEntry(kv._2.asInstanceOf[Array[Byte]]))
        kv = parser.getNext()
      }
    })
    txn.commit() // exception here will get caught above
  }

  def bulkPut(records:Seq[PutRequest]):Unit = {
    val txn = db.getEnvironment.beginTransaction(null, null)
    var reccount = 0
    records.foreach(rec => { db.put(txn, new DatabaseEntry(rec.key), new DatabaseEntry(rec.value.get)); reccount+=1})
    txn.commit()  // exception here will get caught above
  }

  def getRange(minKey:Option[Array[Byte]], maxKey:Option[Array[Byte]], limit:Option[Int], offset:Option[Int], ascending:Boolean):Seq[Record] = {
    logger.debug("[%s] GetRangeRequest: [%s, %s)", this, JArrays.toString(minKey.orNull), JArrays.toString(maxKey.orNull))
    val records = new ArrayBuffer[Record]
    limit.map(records.sizeHint(_))
    iterateOverRange(minKey, maxKey, limit, offset, ascending)((key, value, _) => {
      records += Record(key.getData, value.getData)
    })
    // reccount commented out b/c its not being used and it causes each
    // GetRangeRequest to be a lot slower
    //var reccount = 0
    //iterateOverRange(minKey, maxKey)((_,_,_) => reccount += 1)
    records
  }

  def getBatch(ranges:Seq[StorageMessage]):ArrayBuffer[GetRangeResponse] = {
    val results = new ArrayBuffer[GetRangeResponse]
    results.sizeHint(ranges.size)
    ranges.foreach {
      case GetRangeRequest(minKey, maxKey, limit, offset, ascending) => {
        val records = new ArrayBuffer[Record]
        limit.map(records.sizeHint(_))
        iterateOverRange(minKey, maxKey, limit, offset, ascending)((key, value, _) => {
          records += Record(key.getData, value.getData)
        })
        results += GetRangeResponse(records)
      }
      case _ => throw new RuntimeException("BatchRequests only implemented for GetRange")
    }
    results
  }

  def countRange(minKey:Option[Array[Byte]], maxKey:Option[Array[Byte]]):Int = {
    var count = 0
    iterateOverRange(minKey, maxKey)((_,_,_) => count += 1)
    count
  }

  def copyData(src:PartitionService, overwrite:Boolean):Unit = {
    logger.info("Begin CopyDataRequest src = " + src + ", overwrite = " + overwrite)

    val txn = db.getEnvironment.beginTransaction(null, null)
    val dbeExistingValue = new DatabaseEntry
    val dbeKey = new DatabaseEntry
    val dbeValue = new DatabaseEntry
    logger.debug("Opening iterator for data copy")

    val iter = copyIteratorCtor(src, startKey, endKey) 

    logger.debug("Beginning copy")
    var numRec = 0
    iter.foreach(rec => {
      dbeKey.setData(rec.key)
      dbeValue.setData(rec.value.get)
      
      if (overwrite) db.put(txn, dbeKey, dbeValue)
      else db.putNoOverwrite(txn, dbeKey, dbeValue)
      // NOTE: was previously the code below, but putNoOverwrite seems
      // to give us the exact semantics we are looking for here (plus
      // possibly more efficient than a get() followed by a put())
      //if(db.get(txn, dbeKey, dbeExistingValue, LockMode.READ_COMMITTED) != OperationStatus.SUCCESS)
      //  db.put(txn, dbeKey, dbeValue)

      numRec += 1
      if (numRec % 10000 == 0)
        logger.info("copied %d records so far".format(numRec))
    })

    logger.info("Finished copying %d records".format(numRec))
    
    logger.debug("Beginning commit")
    txn.commit()
    logger.debug("Ending commit")
  }
                         
  def getResponsibility():(Option[Array[Byte]],Option[Array[Byte]]) = (startKey, endKey)

  def applyAggregate(groups:Seq[String],
                     keyType:String,
                     valType:String,
                     filters:Seq[AggFilter],
                     aggs:Seq[AggOp]):Seq[GroupedAgg] = {
    val filterFunctions = filters.map(f => {
      val ois = new java.io.ObjectInputStream(new java.io.ByteArrayInputStream(f.obj))
      ois.readObject.asInstanceOf[Filter[ScalaSpecificRecord]]
    })
    var filterPassed = true

    val aggregates =
      aggs.map(aggOp => {
        val ois = new java.io.ObjectInputStream(new java.io.ByteArrayInputStream(aggOp.obj))
        ois.readObject.asInstanceOf[RemoteAggregate[ScalaSpecificRecord,ScalaSpecificRecord,ScalaSpecificRecord]]
      })
            

    val remKey = Class.forName(keyType).newInstance().asInstanceOf[ScalaSpecificRecord]
    val remVal = Class.forName(valType).newInstance().asInstanceOf[ScalaSpecificRecord]
    
    val groupSchemas = groups.map(valueSchema.getField(_).schema)
    val groupInts = groups.map(valueSchema.getField(_).pos)
    
    var groupMap:Map[CharSequence, scala.collection.mutable.ArraySeq[ScalaSpecificRecord]] = null
    var groupBytesMap:Map[CharSequence, Array[Byte]] = null
    var groupKey:CharSequence = null
    
    var aggVals:Seq[ScalaSpecificRecord] = null
    if (groups.size() == 0) { // no groups, so let's init values now
      aggVals = aggregates.map(_.init())
    }
    else {
      groupMap = new scala.collection.immutable.HashMap[CharSequence,scala.collection.mutable.ArraySeq[ScalaSpecificRecord]]
      groupBytesMap = new scala.collection.immutable.HashMap[CharSequence, Array[Byte]]
    }
    
    var curAggVal =
      if (groups.size() == 0)
        scala.collection.mutable.ArraySeq(aggVals:_*)
      else
        null

    var stop = false

    iterateOverRange(None,None)((key, value, _) => {
      // TODO: Use lazy values here
      val valBytes = value.getData
      remVal.parse(new java.io.ByteArrayInputStream(valBytes,16,(valBytes.length-16)))
      val keyBytes = key.getData
      remKey.parse(keyBytes)
      
      filterPassed = true
      filterFunctions.foreach(ff => { 
        if (filterPassed) { // once one failed just ignore the rest
          filterPassed = ff.applyFilter(remVal)
        }
      })
      if (filterPassed) {  // record passes filters
        if (groups.size() != 0) {
          groupKey = AnalyticsUtils.getGroupKey(groupInts,remVal)
          curAggVal = groupMap.get(groupKey) match {
            case Some(thing) => thing
            case None => { // this is a new group, so init the starting values for the agg
              groupBytesMap += ((groupKey,AnalyticsUtils.getGroupBytes(groupInts,groupSchemas,remVal)))
              scala.collection.mutable.ArraySeq(aggregates.map(_.init()):_*)
            }
          }
        }
        stop = true
        aggregates.view.zipWithIndex foreach(aggregate => {
          curAggVal(aggregate._2) = aggregate._1.applyAggregate(curAggVal(aggregate._2),remKey,remVal)
          stop &= aggregate._1.stop
        })
        if (groups.size() != 0)
          groupMap += ((groupKey,curAggVal))
        if (stop)
          iterateRangeBreakable.break
      }
    })

    if (groups.size() == 0) {
      List(GroupedAgg(None,curAggVal.map(_.asInstanceOf[ScalaSpecificRecord].toBytes)))
    } else {
      groupMap.map(kv => {
        GroupedAgg(groupBytesMap(kv._1),kv._2.map(_.asInstanceOf[ScalaSpecificRecord].toBytes))
      }).toSeq
    }
  }

  def cursorScan(optCursorId:Option[Int], recsPerMessage:Int):(Option[Int],IndexedSeq[Record]) = {
    if (recsPerMessage <= 0) {
      throw new RequestRejectedException("Recs per message needs to be > 0: %d".format(recsPerMessage))
    }

    val dummy = CursorTimeoutRunnable.runnable // initialize it (lazily)

    //logger.info("CursorScanRequest: %s", msg)

    val (cursorId, cursorCtx) = optCursorId.map(id => {
      val ctx = Option(openCursors.get(id)).getOrElse({ 
        throw new RequestRejectedException("Invalid cursor Id: %d".format(id))
      })
      (id, ctx)
    }).getOrElse({
      val id = cursorIdGen.getAndIncrement()
      val txn = db.getEnvironment.beginTransaction(null, null)
      val newCursor = db.openCursor(txn, CursorConfig.READ_UNCOMMITTED)
      initCursor(newCursor)
      val ctx = CursorContext(id, newCursor, txn)
      openCursors.put(id, ctx)
      logger.info("made new cursor with id %d, txn with id %d", id, txn.getId)
      (id, ctx)
    })
    
    // lock the cursor context
    val (moreRecs, recs) = cursorCtx.synchronized {
      // check to see if the cursor is still valid
      if (!cursorCtx.isValid) {
        openCursors.remove(cursorId) // just to be sure 
        throw new RequestRejectedException("Cursor ID: %d has already timedout".format(cursorId))
      }
      cursorCtx.updateActivity() // set a new timeout
      val cursor = cursorCtx.cursor
      val txn = cursorCtx.txn
      val (moreRecs, recs) = advanceCursor(cursor, recsPerMessage)
      if (!moreRecs) { // done, close this cursor up
        cursor.close()
        txn.commit()
        logger.info("closing cursor %d and commit txn %d", cursorId, txn.getId)
        cursorCtx.markInvalid()
        openCursors.remove(cursorId)
      }
      (moreRecs, recs)
    }
    //logger.info("read %d recs, moreRecs = %s", recs.size, moreRecs)
    
    (if (moreRecs) Some(cursorId) else None, recs)
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
    iterateOverRange(lowerKey, upperKey, txn = txn)((_, _, cursor) => {
      cursor.delete()
    })
  }

  private def initCursor(cursor: Cursor) = {
    val dbeKey = new DatabaseEntry
    val dbeValue = new DatabaseEntry
    cursor.getFirst(dbeKey, dbeValue, null)
  }

  /** returns (does the cursor still have more elements?, buffer) */
  private def advanceCursor(cursor: Cursor, recsPerMessage: Int): (Boolean, IndexedSeq[Record]) = {
    val dbeKey = new DatabaseEntry
    val dbeValue = new DatabaseEntry
    val buf = new ArrayBuffer[Record]
    buf.sizeHint(recsPerMessage)
    var stat = cursor.getCurrent(dbeKey, dbeValue, null)
    var recs = 0
    while (stat == OperationStatus.SUCCESS && recs < recsPerMessage) {
      buf += Record(dbeKey.getData, dbeValue.getData)
      recs += 1
      stat = cursor.getNext(dbeKey, dbeValue, null)
    }
    val ret = (stat == OperationStatus.SUCCESS, buf.toIndexedSeq)
    assert(!ret._1 || ret._2.size == recsPerMessage)
    ret
  }

  /**
   * Low level method to iterate over a given range on the database. it is up
   * to the caller to validate minKey and maxKey. The range iterated over is
   * [minKey, maxKey), with the order specified by ascending (and limits
   * respected)
   */
  private def iterateOverRange(minKey: Option[Array[Byte]], 
                               maxKey: Option[Array[Byte]], 
                               limit: Option[Int] = None, 
                               offset: Option[Int] = None, 
                               ascending: Boolean = true, 
                               txn: Option[Transaction] = None)
      (func: (DatabaseEntry, DatabaseEntry, Cursor) => Unit): Unit = {
    val (dbeKey, dbeValue) = (new DatabaseEntry, new DatabaseEntry)
    val cur = db.openCursor(txn.orNull, CursorConfig.READ_UNCOMMITTED)

    var status: OperationStatus = (ascending, minKey, maxKey) match {
      case (true, None, _) => cur.getFirst(dbeKey, dbeValue, null)
      case (true, Some(startKey), _) => cur.getSearchKeyRange(new DatabaseEntry(startKey), dbeValue, null)
      case (false, _, None) => cur.getLast(dbeKey, dbeValue, null)
      case (false, _, Some(startKey)) => {
        // Check if maxKey is past the last key in the database, if so start from the end
        if(cur.getSearchKeyRange(new DatabaseEntry(startKey), dbeValue, null) == OperationStatus.NOTFOUND)
          // no need to skip back one since the maxKey was not found anyways
          cur.getLast(dbeKey, dbeValue, null)
        else {
          // need to check that the cursor is pointing to the first key that
          // is LESS THAN maxKey. getSearchKeyRange semantics only guarantee
          // that the cursor is pointing to the smallest key >= maxKey
          var status = cur.getCurrent(dbeKey, dbeValue, null)
          if(status == OperationStatus.SUCCESS && compare(startKey, dbeKey.getData) <= 0)
            status = cur.getPrev(dbeKey, dbeValue, null)
          status
        }
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

      if (status == OperationStatus.SUCCESS)
        status = cur.getCurrent(dbeKey, dbeValue, null)
      iterateRangeBreakable.breakable { // used to allow func to break out early
        while(status == OperationStatus.SUCCESS &&
              limit.map(_ > returnedCount).getOrElse(true) &&
              maxKey.map(mk => compare(dbeKey.getData, mk) < 0 /* Exclude maxKey from range */).getOrElse(true)) {
                func(dbeKey, dbeValue, cur)
                returnedCount += 1
                status = cur.getNext(dbeKey, dbeValue, null)
              }
      }
    }
    else {
      while(toSkip > 0 && status == OperationStatus.SUCCESS) {
        status = cur.getPrev(dbeKey, dbeValue, null)
        toSkip -= 1
      }

      if (status == OperationStatus.SUCCESS)
        status = cur.getCurrent(dbeKey, dbeValue, null)
      iterateRangeBreakable.breakable { // used to allow func to break out early
        while(status == OperationStatus.SUCCESS &&
              limit.map(_ > returnedCount).getOrElse(true) &&
              minKey.map(compare(_, dbeKey.getData) <= 0).getOrElse(true)) {
                func(dbeKey, dbeValue,cur)
                returnedCount += 1
                status = cur.getPrev(dbeKey, dbeValue, null)
              }
      }
    }
    cur.close()
  }
}
