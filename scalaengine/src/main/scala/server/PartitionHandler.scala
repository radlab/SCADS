package edu.berkeley.cs.scads.storage

import edu.berkeley.cs.scads.comm._
import net.lag.logging.Logger
import scala.collection.mutable.ArrayBuffer
import java.io.{BufferedReader, ObjectInputStream, InputStream, InputStreamReader, ByteArrayInputStream}
import java.util.concurrent.atomic.AtomicInteger


// keep track of number of gets and puts
case class PartitionWorkloadStats(var gets: Int, var puts: Int)

// Used for bulk puts of urls
abstract trait RecParser extends Serializable {
  def setLocation(location: String): Unit = {}

  def setInput(in: InputStream): Unit

  def getNext(): (AnyRef, AnyRef)
}

class RequestRejectedException(e: String) extends Exception(e)

/* Everything at this level is done in Array[Byte] packets as all these methods
 * get called as a result of receiving Avro messages and their responses
 * will also go out as Avro messages.  Converting these bytes to an internal
 * representation, or using them as is, is left to the StorageManager */
abstract trait StorageManager {
  def get(key: Array[Byte]): Option[Array[Byte]]

  def put(key: Array[Byte], value: Option[Array[Byte]]): Unit

  def testAndSet(key: Array[Byte], value: Option[Array[Byte]], expectedValue: Option[Array[Byte]]): Boolean

  // bit odd to have PutRequest in here, but would probable be a performance hit to do it another way
  def bulkUrlPut(parser: RecParser, locations: Seq[String])

  def bulkPut(records: Seq[PutRequest])

  // cursorscanrequest?
  def getRange(minKey: Option[Array[Byte]], maxKey: Option[Array[Byte]], limit: Option[Int], offset: Option[Int], ascending: Boolean): Seq[Record]

  def getBatch(ranges: Seq[MessageBody]): ArrayBuffer[GetRangeResponse]

  def countRange(minKey: Option[Array[Byte]], maxKey: Option[Array[Byte]]): Int

  def copyData(src: PartitionService, overwrite: Boolean)

  def getResponsibility(): (Option[Array[Byte]], Option[Array[Byte]])

  //def deleteRange(minKey: Option[Array[Byte]], maxKey: Option[Array[Byte]], txn: Option[Transaction])
  def applyAggregate(groups: Seq[String],
                     keyType: String,
                     valType: String,
                     filters: Seq[AggFilter],
                     aggregates: Seq[AggOp]): Seq[GroupedAgg]

  // For transactions
  def accept(xid: ScadsXid, updates: Seq[RecordUpdate]): Option[Seq[(Array[Byte], CStruct)]] = None

  def commit(xid: ScadsXid, updates: Seq[RecordUpdate]): Boolean = false

  def abort(xid: ScadsXid) = {}

  def startup(): Unit

  def shutdown(): Unit
}

case class PartitionHandler(manager: StorageManager, trxManager : TrxManager) extends ServiceHandler[PartitionServiceOperation] {

  protected val logger = Logger("PartitionHandler")

  protected def startup(): Unit = manager.startup()

  protected def shutdown(): Unit = manager.shutdown()

  // workload stats code
  protected var currentStats = PartitionWorkloadStats(0, 0)
  protected var completedStats = PartitionWorkloadStats(0, 0)
  private var statsClearedTime = System.currentTimeMillis
  protected val statWindowTime = 20 * 1000 // ms, how long a window to maintain stats for
  protected val clearStatWindowsTime = 60 * 1000 // ms, keep long to keep stats around, in all windows
  protected var statWindows = (0 until clearStatWindowsTime / statWindowTime)
    .map {
    _ => (new AtomicInteger, new AtomicInteger)
  }.toList // (get,put) for each window
  private val getSamplingRate = 1.0
  private val putSamplingRate = 1.0
  private val samplerRandom = new java.util.Random

  def getWorkloadStats(): (PartitionWorkloadStats, Long) = (completedStats, statsClearedTime)

  /**
   * set the current stats as the last completed interval, used when stats are queried
   * zero out the current stats to start a new interval
   * return the old completed interval for archiving
   */
  def resetWorkloadStats(): PartitionWorkloadStats = {
    val ret = completedStats
    completedStats = currentStats
    statsClearedTime = System.currentTimeMillis()
    currentStats = PartitionWorkloadStats(0, 0)
    ret
  }

  @inline private def incrementGetCount(num: Int) = currentStats.gets += num

  @inline private def incrementPutCount(num: Int) = currentStats.puts += num

  // end workload stats stuff

  protected def process(src: Option[RemoteActorProxy], msg: PartitionServiceOperation): Unit = {
    def reply(msg: MessageBody) = src.foreach(_ ! msg)
    try {
      msg match {
        case GetRequest(key) => {
          if (samplerRandom.nextDouble <= getSamplingRate) incrementGetCount(1)
          reply(GetResponse(manager.get(key)))
        }
        case PutRequest(key, value) => {
          manager.put(key, value)
          if (samplerRandom.nextDouble <= putSamplingRate) incrementPutCount(1)
          reply(PutResponse())
        }
        case BulkUrlPutReqest(parserBytes, locations) => {
          val ois = new ObjectInputStream(new ByteArrayInputStream(parserBytes))
          val parser = ois.readObject.asInstanceOf[RecParser]
          manager.bulkUrlPut(parser, locations)
          reply(BulkPutResponse())
        }
        case BulkPutRequest(records) => {
          manager.bulkPut(records)
          /*if (samplerRandom.nextDouble <= putSamplingRate) incrementPutCount(reccount)*/
          reply(BulkPutResponse())
        }
        case GetRangeRequest(minKey, maxKey, limit, offset, ascending) => {
          if (samplerRandom.nextDouble <= getSamplingRate) incrementGetCount(1 /*reccount*/)
          reply(GetRangeResponse(manager.getRange(minKey, maxKey, limit, offset, ascending)))
        }
        case BatchRequest(ranges) =>
          reply(BatchResponse(manager.getBatch(ranges)))
        case CountRangeRequest(minKey, maxKey) => reply(CountRangeResponse(manager.countRange(minKey, maxKey)))
        case TestSetRequest(key, value, expectedValue) => reply(TestSetResponse(manager.testAndSet(key, value, expectedValue)))
        case CursorScanRequest(optCursorId, recsPerMessage) => {
          manager match {
            case bsm: BdbStorageManager => {
              val (cid, recs) = bsm.cursorScan(optCursorId, recsPerMessage)
              reply(CursorScanResponse(cid, recs))
            }
            case _ => src.foreach(_ ! ProcessingException("CursorScan only available for BdbStorageManagers", ""))
          }
        }
        case CopyDataRequest(src, overwrite) => {
          manager.copyData(src, overwrite)
          reply(CopyDataResponse())
        }
        case GetResponsibilityRequest() => {
          val resp = manager.getResponsibility()
          reply(GetResponsibilityResponse(resp._1, resp._2))
        }
        case GetWorkloadStats() => {
          val (stats, clearedTime) = getWorkloadStats()
          reply(GetWorkloadStatsResponse(stats.gets, stats.puts, clearedTime))
        }
        case AggRequest(groups, keyType, valType, filters, aggs) =>
          reply(AggReply(manager.applyAggregate(groups, keyType, valType, filters, aggs)))

        case msg : TrxMessage =>  trxManager.process(src, msg)
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

