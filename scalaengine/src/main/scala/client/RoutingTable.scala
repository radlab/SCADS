package edu.berkeley.cs.scads.storage

import edu.berkeley.cs.scads.comm._
import org.apache.avro.generic.IndexedRecord

/* TODO: Stack RepartitioningProtocol on Routing Table to build working implementation */
trait RepartitioningProtocol[KeyType <: IndexedRecord] extends RoutingTable[KeyType] {
  override def splitPartition(splitPoint: KeyType): List[PartitionService] = throw new RuntimeException("Unimplemented")
  override def mergePartitions(mergeKey: KeyType): Unit = throw new RuntimeException("Unimplemented")
  override def replicatePartition(splitPoint: KeyType, storageHandler: StorageService): PartitionService = throw new RuntimeException("Unimplemented")
  override def deleteReplica(splitPoint: KeyType, partitionHandler: PartitionService): Unit = throw new RuntimeException("Unimplemented")
}

trait RoutingTable[KeyType <: IndexedRecord]  {
  /* Handle to the root node of the scads namespace */
  protected val nsNode: ZooKeeperProxy#ZooKeeperNode

  case class KeyRange(startKey: Option[KeyType], endKey: Option[KeyType])
  case class Partition(range: KeyRange, servers: List[PartitionService])

  def serversForKey(key: KeyType): List[PartitionService] = throw new RuntimeException("Unimplemented")
  def serversForRange(startKey: Option[KeyType], endKey: Option[KeyType]): List[(KeyRange, List[RemoteActor])] = throw new RuntimeException("Unimplemented")

  /* Zookeeper functions */
  def refresh(): Unit = throw new RuntimeException("Unimplemented")
  def expired(): Unit = throw new RuntimeException("Unimplemented")

  /* Returns the newly created PartitionServices from the split */
  def splitPartition(splitPoint: KeyType): List[PartitionService] = throw new RuntimeException("Unimplemented")
  def mergePartitions(mergeKey: KeyType): Unit = throw new RuntimeException("Unimplemented")
  def replicatePartition(splitPoint: KeyType, storageHandler: StorageService): PartitionService = throw new RuntimeException("Unimplemented")
  def deleteReplica(splitPoint: KeyType, partitionHandler: PartitionService): Unit = throw new RuntimeException("Unimplemented")
  def partitions: List[Partition] = throw new RuntimeException("Unimplemented")
}
