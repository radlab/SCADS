package edu.berkeley.cs.scads.storage

import edu.berkeley.cs.avro.marker._

import edu.berkeley.cs.scads.comm._
import edu.berkeley.cs.scads.util._

import org.apache.avro._
import generic._
import specific._

import java.util.Comparator

// TODO: change Manifest to ClassManifest

class GenericNamespace(
    val name: String,
    val cluster: ScadsCluster,
    val root: ZooKeeperProxy#ZooKeeperNode,
    val keySchema: Schema,
    val valueSchema: Schema)
  extends Namespace 
  with RangeKeyValueStore[GenericRecord, GenericRecord]
  with AvroGenericKeyValueSerializer
  with QuorumRangeProtocol
  with DefaultKeyRangeRoutable
  with ZooKeeperGlobalMetadata
  with SimpleRecordMetadata {

  /** this is a hack for now, to handle integration with the director */
  def typedRoutingTable: RangeTable[GenericRecord, PartitionService] = {
    val _rt = routingTable
    val newRTable = _rt.rTable.map(rt => new RangeType[GenericRecord, PartitionService](rt.startKey.map(bytesToKey), rt.values))
    val keyComp = new Comparator[RangeType[GenericRecord, PartitionService]] {
      def compare(
          lhs: RangeType[GenericRecord, PartitionService], 
          rhs: RangeType[GenericRecord, PartitionService]): Int = {
        _rt.keyComparator.compare(
          new RangeType[Array[Byte], PartitionService](lhs.startKey.map(keyToBytes), lhs.values),
          new RangeType[Array[Byte], PartitionService](rhs.startKey.map(keyToBytes), rhs.values)
        ) 
      }
    }
    new RangeTable[GenericRecord, PartitionService](newRTable, keyComp, _rt.mergeCondition)
  }

  val statslogger = net.lag.logging.Logger("statsprotocol")

  /**
  * For each range represented by me, get the worklad stats from all the partitions
  * Add stats from partitions that cover the same range, dividing by the quorum amount
  * (Do this to get "logical" workload to a partition)
  * Return mapping of startkey to tuple of (gets,puts)
  * TODO: is this assuming that the read/write quorum will never change?
  */
  def getWorkloadStats(time:Long):Map[Option[GenericRecord], (Long,Long)] = {
    var haveStats = false
    val ranges = serversForKeyRange(None,None)
    val requests = for (fullrange <- ranges) yield {
      (fullrange.startKey,fullrange.endKey, fullrange.servers, for (partition <- fullrange.servers) yield {partition !! GetWorkloadStats()} )
    }
    val result = Map( requests.map(range => {
      var gets = 0L; var puts = 0L
      val readDivisor = range._4.size//scala.math.ceil(range._4.size * readQuorum).toLong
      val writeDivisor = range._4.size//scala.math.ceil(range._4.size * writeQuorum).toLong

      range._4.foreach(resp => { val (getcount,putcount,statstime) = resp.get match {
        case r:GetWorkloadStatsResponse => (r.getCount.toLong,r.putCount.toLong,r.statsSince.toLong); case _ => (0L,0L,0L)
        }; gets += getcount; puts += putcount; statslogger.debug("workload stats for: %s",statstime.toString); if (statstime >= time) haveStats = true} )
      (range._1.map(bytesToKey) -> ( gets/readDivisor , puts/writeDivisor ))
    }):_* )

    if (haveStats) result else null
  }
}

class GenericHashNamespace(
    val name: String,
    val cluster: ScadsCluster,
    val root: ZooKeeperProxy#ZooKeeperNode,
    val keySchema: Schema,
    val valueSchema: Schema)
  extends Namespace 
  with KeyValueStore[GenericRecord, GenericRecord]
  with AvroGenericKeyValueSerializer
  with QuorumProtocol
  with DefaultHashKeyRoutable
  with ZooKeeperGlobalMetadata
  with SimpleRecordMetadata

class SpecificNamespace[Key <: SpecificRecord : Manifest, Value <: SpecificRecord : Manifest](
    val name: String,
    val cluster: ScadsCluster,
    val root: ZooKeeperProxy#ZooKeeperNode)
  extends Namespace 
  with RangeKeyValueStore[Key, Value]
  with AvroSpecificKeyValueSerializer[Key, Value]
  with QuorumRangeProtocol
  with DefaultKeyRangeRoutable
  with ZooKeeperGlobalMetadata
  with SimpleRecordMetadata {

  override protected val keyManifest = manifest[Key]
  override protected val valueManifest = manifest[Value] 

  lazy val genericNamespace: GenericNamespace = {
    val generic = new GenericNamespace(name, cluster, root, keySchema, valueSchema)
    generic.open()
    generic
  }
}

class SpecificHashNamespace[Key <: SpecificRecord : Manifest, Value <: SpecificRecord : Manifest](
    val name: String,
    val cluster: ScadsCluster,
    val root: ZooKeeperProxy#ZooKeeperNode)
  extends Namespace 
  with KeyValueStore[Key, Value]
  with AvroSpecificKeyValueSerializer[Key, Value]
  with QuorumProtocol
  with DefaultHashKeyRoutable
  with ZooKeeperGlobalMetadata
  with SimpleRecordMetadata {

  override protected val keyManifest = manifest[Key]
  override protected val valueManifest = manifest[Value] 

}

class PairNamespace[Pair <: AvroPair : Manifest](
    val name: String,
    val cluster: ScadsCluster,
    val root: ZooKeeperProxy#ZooKeeperNode)
  extends Namespace 
  with RecordStore[Pair]
  with AvroPairSerializer[Pair]
  with QuorumRangeProtocol
  with DefaultKeyRangeRoutable
  with ZooKeeperGlobalMetadata
  with SimpleRecordMetadata {
  
  override protected val pairManifest = manifest[Pair]

}
