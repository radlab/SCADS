package edu.berkeley.cs.scads.storage

import scala.collection.mutable.HashMap
import edu.berkeley.cs.scads.comm._
import org.apache.avro.generic.IndexedRecord
import org.apache.log4j.Logger

/**
 * Quorum Protocol
 */
abstract class QuorumProtocol[KeyType <: IndexedRecord, ValueType <: IndexedRecord]
      (namespace:String,
       timeout:Int,
       root: ZooKeeperProxy#ZooKeeperNode) (implicit cluster : ScadsCluster)
              extends Namespace[KeyType, ValueType](namespace, timeout, root)(cluster) {
  protected val logger = Logger.getLogger("Namespace")
  protected def serversForKey(key: KeyType): List[PartitionService]

  def writeQurorumForKey(key: KeyType):(List[PartitionService], Int) = {
    val servers = serversForKey(key)
    (servers, servers.size)
  }

  def readQurorumForKey(key: KeyType):(List[PartitionService], Int) = {
    val servers = serversForKey(key)
    (servers, servers.size)
  }

  def put[K <: KeyType, V <: ValueType](key: K, value: Option[V]): Unit = {
    val (servers,size) = writeQurorumForKey(key)
    val putRequest = PutRequest(serializeKey(key), value.map(serializeValue))
    val responses = serversForKey(key).map(_ !! putRequest)
    responses.blockFor(servers.size)
  }

  def get[K <: KeyType](key: K): Option[ValueType] = {
    val (servers, size) = readQurorumForKey(key)
    val getRequest = GetRequest(serializeKey(key))
    val responses = servers.map(_ !! getRequest)
    val votes = new HashMap[Option[ValueType], Int]

    responses.foreach(mf => {
      val value = mf() match {
        case GetResponse(v) => v map deserializeValue
        case u => throw new RuntimeException("Unexpected message during get.")
      }
      votes += ((value, votes.getOrElse(value, 0) + 1))
      if(votes(value) >= size) {
        return value
      }
    })

    throw new RuntimeException("NO QUORUM MET")
  }

  def getPrefix[K <: KeyType](key: K, prefixSize: Int, limit: Option[Int] = None, ascending: Boolean = true):Seq[(KeyType,ValueType)] = throw new RuntimeException("Unimplemented")
  def getRange(start: Option[KeyType], end: Option[KeyType], limit: Option[Int] = None, offset: Option[Int] = None, backwards:Boolean = false): Seq[(KeyType,ValueType)] = throw new RuntimeException("Unimplemented")
  def size():Int = throw new RuntimeException("Unimplemented")
  def ++=(that:Iterable[(KeyType,ValueType)]): Unit = throw new RuntimeException("Unimplemented")

  override def load() : Unit = throw new RuntimeException("Unimplemented")

}
