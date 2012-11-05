package edu.berkeley.cs.scads.storage

import edu.berkeley.cs.scads.comm._
import net.lag.logging.Logger
import scala.collection.mutable.ArrayBuffer
import java.io.{BufferedReader,ObjectInputStream,InputStream,InputStreamReader,ByteArrayInputStream}
import java.util.concurrent.atomic._
import java.util.concurrent._
import scala.collection.JavaConversions._


// Used for bulk puts of urls
abstract trait RecParser extends Serializable {
  def setLocation(location:String):Unit = {}
  def setInput(in:InputStream):Unit
  def getNext():(AnyRef,AnyRef)
}

class RequestRejectedException(e:String) extends Exception(e)

/* Everything at this level is done in Array[Byte] packets as all these methods
 * get called as a result of receiving Avro messages and their responses
 * will also go out as Avro messages.  Converting these bytes to an internal
 * representation, or using them as is, is left to the StorageManager */
abstract trait StorageManager {
  def get(key:Array[Byte]):Option[Array[Byte]]
  def put(key:Array[Byte],value:Option[Array[Byte]]):Unit
  def incrementField(key: Array[Byte], fieldName: String, amount: Int): Unit
  def asyncTopK(minKey: Option[Array[Byte]], maxKey: Option[Array[Byte]], orderingFields: Seq[String], k: Int, ascending: Boolean = false): ScadsFuture[Seq[Record]]
  def groupedTopK(startKey: Option[Array[Byte]], endKey: Option[Array[Byte]], nsAddress: String, groupFields: Seq[String], orderingFields: Seq[String], k: Int, ascending: Boolean): Unit
  def topK(minKey: Option[Array[Byte]], maxKey: Option[Array[Byte]], orderingFields: Seq[String], k: Int, ascending: Boolean = false): Seq[Record] = asyncTopK(minKey, maxKey, orderingFields, k, ascending).get

  def testAndSet(key:Array[Byte], value:Option[Array[Byte]], expectedValue:Option[Array[Byte]]):Boolean
  // bit odd to have PutRequest in here, but would probable be a performance hit to do it another way
  def bulkUrlPut(parser:RecParser, locations:Seq[String])
  def bulkUpdate(updates:Seq[BulkRequest])
  // cursorscanrequest?
  def getRange(minKey:Option[Array[Byte]], maxKey:Option[Array[Byte]], limit:Option[Int], offset:Option[Int], ascending:Boolean):Seq[Record]
  def getBatch(ranges:Seq[StorageMessage]):ArrayBuffer[GetRangeResponse]
  def countRange(minKey:Option[Array[Byte]], maxKey:Option[Array[Byte]]):Int
  def copyData(src:PartitionService, overwrite:Boolean)
  def getResponsibility():(Option[Array[Byte]],Option[Array[Byte]])
  //def deleteRange(minKey: Option[Array[Byte]], maxKey: Option[Array[Byte]], txn: Option[Transaction])
  def applyAggregate(groups:Seq[String],
                     keyType:String,
                     valType:String,
                     filters:Seq[AggFilter],
                     aggregates:Seq[AggOp]):Seq[GroupedAgg]
  def startup():Unit
  def shutdown():Unit
}


case class PartitionHandler(manager:StorageManager) extends ServiceHandler[StorageMessage] {

  protected val logger = Logger("PartitionHandler")

  protected def startup():Unit = manager.startup()
  protected def shutdown():Unit = manager.shutdown()

  def registry = StorageRegistry

  // workload stats code
  @volatile protected var completedStats = {
    val st = GetWorkloadStatsResponse(0,0,0,0,0)
    st.countKeys = List()
    st.countValues = List()
    st
  }
  protected val statWindowTime = 20*1000 // ms, how long a window to maintain stats for
  protected val clearStatWindowsTime = 60*1000 // ms, keep long to keep stats around, in all windows

  val putCount = new AtomicInteger()
  val getCount = new AtomicInteger()
  val getRangeCount = new AtomicInteger()
  val bulkCount = new AtomicInteger()

  def getWorkloadStats() = completedStats

  /**
  * set the current stats as the last completed interval, used when stats are queried
  * zero out the current stats to start a new interval
  * return the old completed interval for archiving
  */
  def resetWorkloadStats(interval: Long) = {
    val oldCounters = counters.getAndSet(new ConcurrentHashMap[String,AtomicInteger]())
    val stats = GetWorkloadStatsResponse(
      getCount.getAndSet(0),
      getRangeCount.getAndSet(0),
      putCount.getAndSet(0),
      bulkCount.getAndSet(0),
      interval)
    val tuples = oldCounters.map(t => (t._1, t._2.get)).toList
    // HACK since we can't serialize sequences of tuples yet
    stats.countKeys = tuples.map(_._1)
    stats.countValues = tuples.map(_._2)
    completedStats = stats
  }

  val counters = new AtomicReference(new ConcurrentHashMap[String,AtomicInteger]())
  def recordTrace(tag: Option[String], op: String) = if (tracingEnabled) {
    val id = op + ":" + tag.getOrElse("unknown")
    val ctr = counters.get.putIfAbsent(id, new AtomicInteger(1))
    if (ctr != null) {
      ctr.getAndIncrement
    }
  }

  protected def process(src: Option[RemoteServiceProxy[StorageMessage]], msg: StorageMessage): Unit = {
    def reply(msg: StorageMessage) = src.foreach(_ ! msg)
    try {
      msg match {
        case GetRequest(key, tag) => {
          getCount.incrementAndGet()
          recordTrace(tag, "get")
          reply(GetResponse(manager.get(key)))
        }
        case PutRequest(key,value,tag) => {
          putCount.incrementAndGet()
          recordTrace(tag, "put")
          manager.put(key,value)
          reply(PutResponse())
        }
        case IncrementFieldRequest(key, fieldName, amount, tag) => {
          recordTrace(tag, "incr")
          reply(IncrementFieldResponse())
        }
        case TopKRequest(startKey, endKey, orderingFields, k, ascending) => {
          reply(TopKResponse(manager.topK(startKey, endKey, orderingFields, k, ascending)))
        }
        case GroupedTopKRequest(startKey, endKey, nsAddress, groupFields, orderingFields, k, ascending) => {
          manager.groupedTopK(startKey, endKey, nsAddress, groupFields, orderingFields, k, ascending)
          reply(GroupedTopKResponse())
        }
        case BulkUrlPutReqest(parserBytes, locations) => {
          val ois = new ObjectInputStream(new ByteArrayInputStream(parserBytes))
          val parser = ois.readObject.asInstanceOf[RecParser]
          manager.bulkUrlPut(parser,locations)
          reply(BulkUpdateResponse())
        }
        case BulkUpdateRequest(updates, tag) => {
          bulkCount.incrementAndGet()
          recordTrace(tag, "bulkUpdate")
          manager.bulkUpdate(updates)
          /*if (samplerRandom.nextDouble <= putSamplingRate) incrementPutCount(reccount)*/
          reply(BulkUpdateResponse())
        }
        case GetRangeRequest(minKey, maxKey, limit, offset, ascending, tag) => {
          getRangeCount.incrementAndGet()
          recordTrace(tag, "getRange")
          reply(GetRangeResponse(manager.getRange(minKey,maxKey,limit,offset,ascending)))
        }
        case BatchRequest(ranges) =>
          reply(BatchResponse(manager.getBatch(ranges)))
        case CountRangeRequest(minKey,maxKey) => reply(CountRangeResponse(manager.countRange(minKey,maxKey)))
        case TestSetRequest(key, value, expectedValue) => reply(TestSetResponse(manager.testAndSet(key,value,expectedValue)))
        case CursorScanRequest(optCursorId, recsPerMessage) => {
          manager match {
            case bsm:BdbStorageManager => {
              val (cid,recs) = bsm.cursorScan(optCursorId, recsPerMessage)
              reply(CursorScanResponse(cid,recs))
            }
            case _ => src.foreach(_ ! ProcessingException("CursorScan only available for BdbStorageManagers","")) 
          }
        }
        case CopyDataRequest(src, overwrite) => {
          manager.copyData(src,overwrite)
          reply(CopyDataResponse())
        }
        case GetResponsibilityRequest() => {
          val resp = manager.getResponsibility()
          reply(GetResponsibilityResponse(resp._1,resp._2))
        }
        case GetWorkloadStats() => {
          reply(completedStats)
        }
        case AggRequest(groups, keyType, valType, filters, aggs) =>
          reply(AggReply(manager.applyAggregate(groups, keyType, valType, filters, aggs)))
        case _ => src.foreach(_ ! ProcessingException("Not Implemented", ""))
      }
    } catch {
      case r: RequestRejectedException => reply(RequestRejected(r.getMessage(), msg))
      case e: Exception => {
        logger.info("Exception processing request")
        e.printStackTrace()
        reply(ProcessingException(e.getMessage, e.getStackTrace.mkString("\n")))
      }
    }
  }
}

