package edu.berkeley.cs.scads.storage.newclient

import edu.berkeley.cs.avro.marker._
import edu.berkeley.cs.scads.comm._

import org.apache.avro.Schema 
import org.apache.avro.generic._

import actors.Actor
import collection.mutable.{ Seq => MutSeq, _ }
import concurrent.ManagedBlocker

import java.util.concurrent.TimeUnit

private[storage] object QuorumProtocol {
  // TODO: fix string hackery?
  val MinString = "" 
  val MaxString = new String(Array.fill[Byte](20)(127.asInstanceOf[Byte]))

  val ZK_QUORUM_CONFIG = "quorumProtConfig"

  val BufSize = 1024
  val FutureCollSize = 1024
}

trait QuorumProtocol 
  extends Protocol
  with Namespace
  with KeyRoutable
  with RecordMetadata
  with GlobalMetadata {

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
      def cancelComputation = error("NOT IMPLEMENTED")
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
      Some(record.map(r => extractMetadataFromValue(r)._2))
    }
  }

  override def putBytes(key: Array[Byte], value: Option[Array[Byte]]): Boolean = {
    val (servers, quorum) = writeQuorumForKey(key)
    val putRequest = PutRequest(key, value)
    val responses = serversForKey(key).map(_ !! putRequest)
    responses.blockFor(quorum)
    true // TODO: do actual return...
  }

  override def putBulkBytes(that: TraversableOnce[(Array[Byte], Array[Byte])]): Unit = {
    /* Buffers KV tuples until the average buffer size for all the servers is
     * greater than BufSize. Then sends the request to the servers, while
     * repeating the buffering process for the next set of KV tuples. When the
     * tuple stream runs out, or when the outstanding future collection grows
     * above a certain threshold, blocks (via quorum) on each of the returned
     * future collections.
     */
    val serverBuffers = new HashMap[PartitionService, ArrayBuffer[PutRequest]]
    val outstandingFutureColls = new ArrayBuffer[FutureCollection]

    def averageBufferSize =
      serverBuffers.values.foldLeft(0)(_ + _.size) / serverBuffers.size.toDouble 

    def writeServerBuffers() {
      outstandingFutureColls += new FutureCollection(serverBuffers.map { tup => 
        tup._1 !! BulkPutRequest(tup._2.toSeq)
      }.toSeq)
      serverBuffers.clear() // clear buffers
    }

    def blockFutureCollections() {
      outstandingFutureColls.foreach(coll => coll.blockFor(scala.math.ceil(coll.futures.size * writeQuorum).toInt))
      outstandingFutureColls.clear() // clear ftch colls
    }

    that.foreach { case (key, value) => 

      val (servers, quorum) = writeQuorumForKey(key)
      val putRequest = PutRequest(key, Some(createMetadata(value)))

      for (server <- servers) {
        val buf = serverBuffers.getOrElseUpdate(server, new ArrayBuffer[PutRequest])
        buf += putRequest
      }

      if (averageBufferSize >= BufSize.toDouble)
        // time to flush server buffers
        writeServerBuffers()

      if (outstandingFutureColls.size > FutureCollSize)
        // time to block on collections (via quorum) so that we don't congest
        // the network
        blockFutureCollections()
    }

    if (!serverBuffers.isEmpty) {
      // outstanding keys since last flush - since we've reached the end of
      // the iterable collection, we must write and block now
      writeServerBuffers()
      blockFutureCollections()
    }
  }

  private var readQuorum: Double = 0.001
  private var writeQuorum: Double = 1.0

  /** Initialization callbacks */

  onOpen {
    getMetadata(ZK_QUORUM_CONFIG) match {
      case Some(bytes) => 
        val config = new QuorumProtocolConfig
        config.parse(bytes)
        readQuorum  = config.readQuorum
        writeQuorum = config.writeQuorum
      case None =>
        // TODO: what to do here?
        throw new RuntimeException("could not find quorum protocol")
    }
  }

  onCreate {
    val config = new QuorumProtocolConfig(readQuorum, writeQuorum)
    putMetadata(ZK_QUORUM_CONFIG, config.toBytes)
  }

  onClose {
    // TODO: what to do here?
  }

  onDelete {
    deleteMetadata(ZK_QUORUM_CONFIG)
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

  @inline private def writeQuorumForKey(key: Array[Byte]): (Seq[PartitionService], Int) = {
    val servers = serversForKey(key)
    (servers, scala.math.ceil(servers.size * writeQuorum).toInt)
  }

  @inline private def readQuorum(nbServers: Int): Int = scala.math.ceil(nbServers * readQuorum).toInt

  @inline private def readQuorumForKey(key: Array[Byte]): (Seq[PartitionService], Int) = {
    val servers = serversForKey(key)
    (servers, readQuorum(servers.size))
  }

  /**
   * Returns all value versions for a given key. Does not perform read-repair.
   */
  def getAllVersions(key: Array[Byte]): Seq[(PartitionService, Option[Array[Byte]])] = {
    val (partitions, quorum) = readQuorumForKey(key)
    val getRequest = GetRequest(key)
    // TODO: should probably scatter and gather here instead of process
    // sequentially
    for (partition <- partitions) yield {
      val values = partition !? getRequest match {
        case GetResponse(optV) =>
          optV.map(v => extractMetadataFromValue(v)._2)
        case u => throw new RuntimeException("Unexpected message during get.")
      }
      (partition, values) 
    }
  }

  class GetHandler(val key: Array[Byte], val futures: Seq[MessageFuture], val timeout: Long = 5000) {
    private val responses = new java.util.concurrent.LinkedBlockingQueue[MessageFuture]
    futures.foreach(_.forward(responses))
    val losers = new ArrayBuffer[RemoteActorProxy] 
    val winners = new ArrayBuffer[RemoteActorProxy]
    var winnerValue: Option[Array[Byte]] = None
    private var ctr = 0
    val startTime = System.currentTimeMillis
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
      val time = startTime - System.currentTimeMillis + timeout
      val future = responses.poll(if (time > 0) time else 0, TimeUnit.MILLISECONDS)
      if (future == null) {
        failed = true
        return
      }
      future() match {
        case m@GetResponse(v) => {
          val cmp = (winnerValue, v) match {
            case (None, None) => 0
            case (None, Some(_)) => -1
            case (Some(_), None) => 1
            case (Some(lhs), Some(rhs)) => compareMetadata(lhs, rhs)
          }
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

  /**
   * ReadRepairer actor - calls to processRest() run in a managed blocker, so that
   * the ForkJoinPool does not starve
   * TODO: Consider replacing this read repair actor with just a standard
   * thread pool
   */
  object ReadRepairer extends ManagedBlockingActor {
    start() /* Auto start */
    
    def act() {
      loop {
        react {
          case handler: GetHandler =>
            managedBlock {
              handler.processRest()
            }
            for (server <- handler.repairList)
              server !! PutRequest(handler.key, handler.winnerValue)
          //case handler: RangeHandle =>
          //  managedBlock {
          //    handler.processRest()
          //  }
          //  for ((key, (data, servers)) <- handler.loosers) {
          //    for (server <- servers) {
          //      server !! PutRequest(key, Some(data))
          //    }
          //  }
          case m => throw new RuntimeException("Unknown message" + m)
        }
      }
    }
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

  /**
   * Actor which makes it easier to invoke managedBlock
   */
  trait ManagedBlockingActor extends Actor {
    protected def managedBlock(f: => Unit) {
      scheduler.managedBlock(new DefaultManagedBlocker(() => f))
    }
  }

}

trait QuorumRangeProtocol 
  extends RangeProtocol
  with QuorumProtocol
  with KeyRangeRoutable {

  override def getKeys(start: Option[Array[Byte]], 
                       end: Option[Array[Byte]], 
                       limit: Option[Int], 
                       offset: Option[Int], 
                       ascending: Boolean): Seq[(Array[Byte], Array[Byte])] = error("getKeys")

}
