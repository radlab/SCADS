package edu.berkeley.cs.scads.storage.newclient

import edu.berkeley.cs.avro.marker._
import edu.berkeley.cs.scads.comm._

import org.apache.avro.Schema 
import org.apache.avro.generic._

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

  override def getBytes(key: Array[Byte]): Option[Array[Byte]] = error("getKey")
  override def putBytes(key: Array[Byte], value: Option[Array[Byte]]): Boolean = {
    val (servers, quorum) = writeQuorumForKey(key)
    val putRequest = PutRequest(key, value)
    val responses = serversForKey(key).map(_ !! putRequest)
    responses.blockFor(quorum)
    true // TODO: do actual return...
  }

  override def putBulkBytes(that: TraversableOnce[(Array[Byte], Array[Byte])]): Unit = error("putKeys")

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
