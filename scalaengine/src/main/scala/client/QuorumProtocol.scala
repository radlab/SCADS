package edu.berkeley.cs.scads.storage

import edu.berkeley.cs.avro.marker._
import edu.berkeley.cs.scads.comm._

import org.apache.avro.Schema 
import org.apache.avro.generic._

import actors.Actor
import collection.mutable.{ Seq => MutSeq, _ }
import concurrent.ManagedBlocker

import java.util.concurrent.TimeUnit

private[storage] object QuorumProtocol {
  val ZK_QUORUM_CONFIG = "quorumProtConfig"

  val BulkPutBufSize = 1024
  val BulkPutMaxOutstanding = 64
  val BulkPutTimeout = 60 * 1000
}

class TimeoutCounter(timeout: Long) {
  val startTime = System.currentTimeMillis()

  def remaining: Long = {
    val remainingTime = timeout - (System.currentTimeMillis - startTime)
    if (remainingTime < 0)
      1
    else if (remainingTime > timeout) /* handle ec2 timeskips */
      timeout
    else
      remainingTime
  }
}

trait QuorumProtocol
  extends Protocol
  with Namespace
  with KeyRoutable
  with RecordMetadata
  with GlobalMetadata
  with ParFuture {

  import QuorumProtocol._

  override def getBytes(key: Array[Byte]): Option[Array[Byte]] = {
    val (ftchs, quorum) = makeGetRequests(key)
    finishGetHandler(new GetHandler(key, ftchs), quorum).getOrElse(throw new RuntimeException("Could not complete get request - not enough servers responded"))
  }

  /**
   * Issues a get request but does not block the current thread waiting for
   * responses. A ScadsFuture is returned which can be used to block for
   * responses. Calls to get/apply on the ScadsFuture are idempotent - the
   * first call will cause the current thread to wait for GetResponse
   * messages, and vote on the winning record, handling the read repair in the
   * process. Subsequent calls (regardless of thread) then wait for the first
   * call to finish and piggyback off the result.
   *
   * NOTE: There *IS* a subtle difference between calling get(key) and calling
   * asyncGet(key).get(), in the case where the servers do not respond to the
   * get request. In the former case, an exception is thrown after a default
   * (for now hardcoded) timeout value expires. In the latter case, the thread
   * is blocked forever. Use with timeouts is recommended.
   *
   * NOTE: The returned future does not implement cancel
   */
  override def asyncGetBytes(key: Array[Byte]): ScadsFuture[Option[Array[Byte]]] = {
    val (ftchs, quorum) = makeGetRequests(key)
    new ComputationFuture[Option[Array[Byte]]] {
      def compute(timeoutHint: Long, unit: TimeUnit) = 
        finishGetHandler(new GetHandler(key, ftchs, unit.toMillis(timeoutHint)), quorum)
          .getOrElse(throw new RuntimeException("Could not complete get request - not enough servers responded"))
      def cancelComputation = sys.error("NOT IMPLEMENTED")
    }
  }

  @inline private def makeGetRequests(key: Array[Byte]) = {
    val (servers, quorum) = readQuorumForKey(key)
    val getRequest = GetRequest(key)
    // (ftch, quorum)
    (servers.map(_ !! getRequest), quorum)
  }

  @inline private def finishGetHandler(handler: GetHandler, quorum: Int): Option[Option[Array[Byte]]] = {
    val record = handler.vote(quorum)
    if (handler.failed) None
    else {
      // TODO: repair on failed case (above) still?
      ReadRepairer ! handler
      Some(record.map(extractRecordFromValue))
    }
  }

  override def asyncPutBytes(key: Array[Byte], value: Option[Array[Byte]]): ScadsFuture[Unit] = {
    val (servers, quorum) = writeQuorumForKey(key)
    val putRequest = PutRequest(key, value.map(createMetadata))
    val responses = serversForKey(key).map(_ !! putRequest)

    new ComputationFuture[Unit] {
      def compute(timeoutHint: Long, unit: TimeUnit) = {
        responses.blockFor(quorum)
      }

      def cancelComputation = sys.error("NOT IMPLEMENTED")
    }
  }

  override def putBytes(key: Array[Byte], value: Option[Array[Byte]]): Unit = {
    val (servers, quorum) = writeQuorumForKey(key)
    val putRequest = PutRequest(key, value.map(createMetadata))
    val responses = serversForKey(key).map(_ !! putRequest)
    responses.blockFor(quorum, 500, TimeUnit.MILLISECONDS)
  }

  /* Buffers KV tuples until the average buffer size for all the servers is
   * greater than BufSize. Then sends the request to the servers, while
   * repeating the buffering process for the next set of KV tuples. When the
   * tuple stream runs out, or when the outstanding future collection grows
   * above a certain threshold, blocks on each of the returned
   * future collections.
   *
   * Note: the current implementation is Write All not quorum based.
   */
  override def putBulkBytes(key: Array[Byte], value: Array[Byte]): Unit = {
    val (servers, quorum) = writeQuorumForKey(key)
    val putRequest = PutRequest(key, Some(createMetadata(value)))

    for (server <- servers) {
      val buf = serverBuffers.getOrElseUpdate(server, new ArrayBuffer[PutRequest](BulkPutBufSize))
      buf += putRequest

      /* send the buffer when its full */
      if (buf.size >= BulkPutBufSize)
        sendBuffer(server, buf)
    }

    if (outstandingPuts.size >= BulkPutMaxOutstanding)
      processNextOutstandingRequest
  }

  override def flushBulkBytes(): Unit = {
    /* flush any remaining send buffers */
    serverBuffers.foreach {
      case (server, buffer) => sendBuffer(server, buffer)
    }

    /* block until we receive responses for all sent requests */
    while (outstandingPuts.size > 0)
      processNextOutstandingRequest
  }

  protected val serverBuffers = new HashMap[PartitionService, ArrayBuffer[PutRequest]]

  protected case class OutstandingPut(timestamp: Long, server: PartitionService, sendBuffer: ArrayBuffer[PutRequest], future: MessageFuture, tries: Int = 0)

  protected val outstandingPuts = new collection.mutable.Queue[OutstandingPut]

  /* send the request and append it to the list of outstanding requests */
  @inline private def sendBuffer(server: PartitionService, sendBuffer: ArrayBuffer[PutRequest], tries: Int = 0): Unit = {
    if (tries > 5)
      throw new RuntimeException("Retries exceeded for server %s".format(server))

    outstandingPuts enqueue OutstandingPut(
      System.currentTimeMillis,
      server,
      sendBuffer,
      server !! BulkPutRequest(sendBuffer),
      tries)
    serverBuffers -= server
  }

  /* wait up to timeout for the oldest request to return if it doesn't resend it*/
  @inline private def processNextOutstandingRequest: Unit = {
    val oldestRequest = outstandingPuts.dequeue
    val remainingTime = BulkPutTimeout - (System.currentTimeMillis - oldestRequest.timestamp)
    val timeout =
      if (remainingTime < 0)
        1
      else if (remainingTime > BulkPutTimeout) /* handle ec2 timeskips */
        BulkPutTimeout
      else
        remainingTime

    oldestRequest.future.get(timeout) match {
      case Some(BulkPutResponse()) => null
      case Some(otherMsg) => {
        logger.warning("Received unexpected message for bulk put to %s: %s. Resending", oldestRequest.server, otherMsg)
        sendBuffer(oldestRequest.server, oldestRequest.sendBuffer, oldestRequest.tries + 1)
      }
      case None => {
        logger.warning("Bulkput to %s timed out. Resending.", oldestRequest.server)
        sendBuffer(oldestRequest.server, oldestRequest.sendBuffer, oldestRequest.tries + 1)
      }
    }
  }


  private var readQuorum: Double = 0.001
  private var writeQuorum: Double = 1.0

  /** Initialization callbacks */

  private def doCreate(): Unit = {
    val config = new QuorumProtocolConfig(readQuorum, writeQuorum)
    putMetadata(ZK_QUORUM_CONFIG, config.toBytes)
  }

  onCreate {
    doCreate()
  }

  onOpen { isNew =>
    if (isNew) doCreate()
    else getMetadata(ZK_QUORUM_CONFIG) match {
      case Some(bytes) => 
        val config = new QuorumProtocolConfig
        config.parse(bytes)
        readQuorum  = config.readQuorum
        writeQuorum = config.writeQuorum
      case None => throw new RuntimeException("could not find quorum protocol")
    }
    isNew
  }

  onClose {
    // no-op 
  }

  onDelete {
    //deleteMetadata(ZK_QUORUM_CONFIG)
  }

  /** NOT thread-safe to set */
  def setReadWriteQuorum(readQuorum: Double, writeQuorum: Double): Unit = {
    require(0 < readQuorum && readQuorum <= 1, "Read quorum has to be in the range 0 < RQ <= 1 but was " + readQuorum)
    require(0 < writeQuorum && writeQuorum <= 1, "Write quorum has to be in the range 0 < WQ <= 1 but was " + writeQuorum)
    require((writeQuorum + readQuorum) >= 1, "Read + write quorum has to be >= 1 but was " + (readQuorum + writeQuorum))
    this.readQuorum = readQuorum
    this.writeQuorum = writeQuorum
    val config = new QuorumProtocolConfig(readQuorum, writeQuorum)
    putMetadata(ZK_QUORUM_CONFIG, config.toBytes)
  }

  protected def writeQuorumForKey(key: Array[Byte]): (Seq[PartitionService], Int) = {
    val servers = serversForKey(key)
    (servers, scala.math.ceil(servers.size * writeQuorum).toInt)
  }

  protected def readQuorum(nbServers: Int): Int = scala.math.ceil(nbServers * readQuorum).toInt

  protected def readQuorumForKey(key: Array[Byte]): (Seq[PartitionService], Int) = {
    val servers = serversForKey(key)
    (servers, readQuorum(servers.size))
  }

  /**
   * Returns all value versions for a given key. Does not perform read-repair.
   */
  def getAllVersions(key: Array[Byte]): Seq[(PartitionService, Option[Array[Byte]])] = {
    val (partitions, quorum) = readQuorumForKey(key)
    val getRequest = GetRequest(key)

    waitForAndThrowException(partitions.map(partition => (partition !! getRequest, partition))) { 
      case (GetResponse(optV), partition) => (partition, optV.map(extractRecordFromValue))
    }
  }

  class GetHandler(val key: Array[Byte], val futures: Seq[MessageFuture], val timeout: Long = 5000) {
    private val responses = new java.util.concurrent.LinkedBlockingQueue[MessageFuture]
    futures.foreach(_.forward(responses))
    val losers = new ArrayBuffer[RemoteActorProxy] 
    val winners = new ArrayBuffer[RemoteActorProxy]
    var winnerValue: Option[Array[Byte]] = None
    private var ctr = 0
    val timeoutCounter = new TimeoutCounter(timeout)
    var failed = false

    def repairList() = losers

    def vote(quorum: Int): Option[Array[Byte]] = {
      (1 to quorum).foreach(_ => compareNext())
      return winnerValue
    }

    def processRest() = {
      for (i <- 1 to futures.length - ctr) {
        compareNext()
      }
    }

    private def compareNext(): Unit = {
      ctr += 1
      val future = responses.poll(timeoutCounter.remaining, TimeUnit.MILLISECONDS)
      if (future == null) {
        failed = true
        return
      }
      future() match {
        case GetResponse(v) => {
          val cmp = optCompareMetadata(winnerValue, v)
          if (cmp > 0) {
            future.source.foreach(losers += _)
          } else if (cmp < 0) {
            losers ++= winners
            winners.clear
            future.source.foreach(winners += _)
            winnerValue = v
          }else{
            future.source.foreach(winners += _)
          }
        }
        case m => throw new RuntimeException("Unknown message " + m)
      }
    }

  }

  protected def optCompareMetadata(optLhs: Option[Array[Byte]], optRhs: Option[Array[Byte]]): Int = (optLhs, optRhs) match {
    case (None, None) => 0
    case (None, Some(_)) => -1
    case (Some(_), None) => 1
    case (Some(lhs), Some(rhs)) => compareMetadata(lhs, rhs)
  }

  protected def readRepairPF: PartialFunction[Any, Unit] = _rrpf 

  private val _rrpf: PartialFunction[Any, Unit] = {
    case handler: GetHandler =>
      readRepairManagedBlock {
        handler.processRest()
      }
      for (server <- handler.repairList)
        server !! PutRequest(handler.key, handler.winnerValue)
  }

  /**
   * ReadRepairer actor - calls to processRest() run in a managed blocker, so that
   * the ForkJoinPool does not starve
   * TODO: Consider replacing this read repair actor with just a standard
   * thread pool
   */
  object ReadRepairer extends Actor {
    start() /* Auto start */
    
    def act() {
      loop {
        react {
          case m =>
            if (readRepairPF.isDefinedAt(m)) readRepairPF.apply(m)
            else throw new RuntimeException("Unknown message" + m)
        }
      }
    }

    /** expose the scheduler for blocks */
    private[storage] def getScheduler = scheduler
  }

  /**
   * Default ManagedBlocker for a function which has no means to query
   * progress
   */
  class DefaultManagedBlocker(f: () => Unit) extends ManagedBlocker {
    private var completed = false
    def block() = {
      f()
      completed = true
      true
    }
    def isReleasable = completed
  }

  protected def readRepairManagedBlock(f: => Unit) {
    ReadRepairer.getScheduler.managedBlock(new DefaultManagedBlocker(() => f))
  }

}

trait QuorumRangeProtocol 
  extends RangeProtocol
  with QuorumProtocol
  with KeyRangeRoutable {

  /** assumes start/end key prepopulated w/ sentinel min/max values */
  private def startGetRangeRequest(startKey: Option[Array[Byte]], endKey: Option[Array[Byte]], limit: Option[Int], offset: Option[Int], ascending: Boolean): (Seq[RangeDesc], Seq[Seq[MessageFuture]]) = {
    val partitions = if (ascending) serversForKeyRange(startKey, endKey) else serversForKeyRange(startKey, endKey).reverse

    limit match {
      case Some(_) => /* if limit is defined, then only send a req to the first server. return a pointer to the tail of the first partition */
        val range = partitions.head
        val rangeRequest = new GetRangeRequest(range.startKey, range.endKey, limit, offset, ascending)
        (partitions.tail, Seq(range.servers.map(_ !! rangeRequest)))
      case None => /* if no limit is defined, then we'll have to send a request to every server. the pointer should be nil since no servers left */
        (Nil, partitions.map(range => {
          val rangeRequest = new GetRangeRequest(range.startKey, range.endKey, limit, offset, ascending)
          range.servers.map(_ !! rangeRequest)
        }).toSeq)
    }
  }

  /**
   * Finish a get range request. Ftchs are the GetRangeRequests which have
   * already been sent out, and partitions are the available servers to pull
   * data from 
   */
  private def finishGetRangeRequest(partitions: Seq[RangeDesc], ftchs: Seq[Seq[MessageFuture]], limit: Option[Int], offset: Option[Int], ascending: Boolean, timeout: Option[Long]): Seq[(Array[Byte], Array[Byte])] = {

    def newRangeHandle(ftchs: Seq[MessageFuture]) = 
      timeout.map(t => new RangeHandle(ftchs, t)).getOrElse(new RangeHandle(ftchs))

    var handlers = ftchs.map(x => newRangeHandle(x)).toBuffer

    var result = new ArrayBuffer[(Array[Byte], Array[Byte])]
    var openRec = limit.map(_.toLong).getOrElse(java.lang.Long.MAX_VALUE)
    var servers = partitions
    var cur = 0
    for (handler <- handlers) {
      if (openRec > 0) {
        val records = handler.vote(readQuorum(handler.futures.size))
        if (handler.failed)
          throw new RuntimeException("Not enough servers responded. We need to throw better exceptions for that case")
        //if we do not get enough records, we request more, before we process the current batch
        if (records.length <= openRec && !servers.isEmpty) {
          val range = servers.head
          val newLimit = if(limit.isDefined) Some(openRec.toInt) else None
          val rangeRequest = new GetRangeRequest(range.startKey, range.endKey, newLimit, offset, ascending)
          handlers.append(newRangeHandle(range.servers.map(_ !! rangeRequest)))
          servers = servers.tail
        }
        result.appendAll(records.flatMap(rec =>
          if (openRec > 0) {
            openRec -= 1
            rec.value.map(v => (rec.key, extractRecordFromValue(v))).toList
          } else
            Nil
          )
        )
      }
    }
    handlers.foreach(ReadRepairer ! _)
    result
  }

  override def getKeys(start: Option[Array[Byte]], 
                       end: Option[Array[Byte]], 
                       limit: Option[Int], 
                       offset: Option[Int], 
                       ascending: Boolean): Seq[(Array[Byte], Array[Byte])] = {
    val (ptr, ftchs) = startGetRangeRequest(start, end, limit, offset, ascending)
    finishGetRangeRequest(ptr, ftchs, limit, offset, ascending, None)
  }

  override def asyncGetKeys(start: Option[Array[Byte]], end: Option[Array[Byte]], limit: Option[Int], offset: Option[Int], ascending: Boolean): ScadsFuture[Seq[(Array[Byte], Array[Byte])]] = {
    val (ptr, ftchs) = startGetRangeRequest(start, end, limit, offset, ascending)
    new ComputationFuture[Seq[(Array[Byte], Array[Byte])]] {
      def compute(timeoutHint: Long, unit: TimeUnit) = 
        finishGetRangeRequest(ptr, ftchs, limit, offset, ascending, Some(unit.toMillis(timeoutHint)))
      def cancelComputation = sys.error("NOT IMPLEMENTED")
    }
  }

  override protected def readRepairPF = _rrpf 

  private val _rrpf: PartialFunction[Any, Unit] = {
    val pf: PartialFunction[Any, Unit] = {
      case handler: RangeHandle =>
        readRepairManagedBlock {
          handler.processRest()
        }
        for ((key, (data, servers)) <- handler.losers) {
          for (server <- servers) {
            server !! PutRequest(key, Some(data))
          }
        }
    }
    pf.orElse(super.readRepairPF)
  }

  /**
   * Tests the range for conflicts and marks all violations for future resolution.
   * This class is optimized for none/few violations.
   */
  class RangeHandle(val futures: Seq[MessageFuture], val timeout: Long = 100000) {
    val timeoutCounter = new TimeoutCounter(timeout)
    val responses = new java.util.concurrent.LinkedBlockingQueue[MessageFuture]
    futures.foreach(_.forward(responses))

    // TODO: hashing on byte array like this is no good. this should be redone
    val losers = new HashMap[Array[Byte], (Array[Byte], List[RemoteActorProxy])]
    val winners = new ArrayBuffer[Record]

    var baseServer: RemoteActorProxy = null //The whole comparison is based on the first response

    val winnerExceptions = new HashMap[Array[Byte], RemoteActorProxy]

    val startTime = System.currentTimeMillis
    var failed = false
    private var ctr = 0

    def vote(quorum: Int): ArrayBuffer[Record] = {
      (1 to quorum).foreach(_ => processNext())
      return winners
    }

    def processRest() = {
      for (i <- 1 to futures.length - ctr) {
        processNext()
      }
    }

    private def processNext(): Unit = {
      ctr += 1
      val future = responses.poll(timeoutCounter.remaining, TimeUnit.MILLISECONDS)
      if (future == null) {
        failed = true
        return
      }
      val result = future() match {
        case GetRangeResponse(v) => v
        case m => throw new RuntimeException("Unexpected Message (was expecting GetRangeResponse): " + m)
      }

      if (winners.isEmpty) {
        //println("first result " + result.map(a => QuorumProtocol.this.deserializeKey(a.key)))
        winners.insertAll(0, result)
        baseServer = future.source.get
      } else {
        merge(result, future.source.get)
      }
    }

    private def merge(newRecords: Seq[Record], newServer: RemoteActorProxy): Unit = {
      var recordPtr = newRecords
      var i = 0
      while (i < winners.length && !recordPtr.isEmpty) {
        val winnerKey = winners(i).key
        val newKey = recordPtr.head.key
        assert(winners(i).value.isDefined) //should always be defined if we get data back, otherwise delete might not work
        val winnerValue = winners(i).value.get
        assert(recordPtr.head.value.isDefined)
        val newValue = recordPtr.head.value.get
        compareKey(winnerKey, newKey) match {
          case cmp if cmp < 0 =>
            i += 1
          case 0 => {
            compareMetadata(winnerValue, newValue) match {
              case -1 => {
                val outdatedServer = winnerExceptions.getOrElse(winnerKey, baseServer)
                losers += winnerKey -> (newValue, outdatedServer :: losers.getOrElse(winnerKey, (null, Nil))._2)
                winnerExceptions += winnerKey -> newServer
                winners.update(i, recordPtr.head)
              }
              case 1 => {
                losers += winnerKey -> (winnerValue, newServer :: losers.getOrElse(winnerKey, (null, Nil))._2)
              }
              case 0 => ()
            }
            recordPtr = recordPtr.tail
            i += 1
          }
          case cmp if cmp > 0 => {
            winners.insert(i, recordPtr.head)
            i += 1 //because of the insert
            winnerExceptions += newKey -> newServer
            recordPtr = recordPtr.tail
          }
        }
      }
    }

  }
 
  /*
   * NB: We trust the caller here that the records produced from the given locations will
   * all have keys in between firstKey and lastKey.  Therefore we just check that
   * firstKey<->lastKey only covers one partition
   */
  override def putBulkLocations(parser:RecParser,locations:Array[String],
                                firstKey:Option[Array[Byte]],lastKey:Option[Array[Byte]]) {
    val partitions = serversForKeyRange(firstKey, lastKey)
    if (partitions.length > 1)
      sys.error("Can't bulk put locations over more than one partition")
    else {
      val baos = new java.io.ByteArrayOutputStream
      val oos = new java.io.ObjectOutputStream(baos)
      oos.writeObject(parser)
      val responses = partitions(0).servers.map(_ !! BulkUrlPutReqest(baos.toByteArray,locations))
      responses.blockFor(partitions(0).servers.length)
    }
  }

  /**
   * Get all value versions for a range of keys.  Does not perform read-repair.
   */
  def getAllRangeVersions(startKey: Option[Array[Byte]], endKey: Option[Array[Byte]]): Seq[(PartitionService, Seq[(Array[Byte],Option[Array[Byte]])])] = {
    val partitions = serversForKeyRange(startKey, endKey)
    waitForAndThrowException(
      partitions.flatMap(range => {
        val rangeReq = GetRangeRequest(range.startKey, range.endKey, None, None, true)
        range.servers.map(partition => (partition !! rangeReq, partition)) 
      }) 
    ) {    
      case (GetRangeResponse(recs), partition) => (partition, recs.map(r=>(r.key,r.value)))
    }
  }

}
