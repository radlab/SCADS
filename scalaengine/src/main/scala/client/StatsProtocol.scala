package edu.berkeley.cs.scads.storage.routing

import edu.berkeley.cs.scads.util.RangeTable
import edu.berkeley.cs.scads.util.RangeType

import collection.mutable.HashMap
import org.apache.avro.generic.IndexedRecord
import collection.immutable.{TreeMap, SortedMap}
import org.apache.avro.Schema
import java.util.{Comparator, Arrays}
import edu.berkeley.cs.scads.storage.Namespace
import org.apache.zookeeper.CreateMode
import edu.berkeley.cs.avro.marker.AvroRecord
import edu.berkeley.cs.avro.runtime._
import edu.berkeley.cs.scads.comm._

abstract trait WorkloadStatsProtocol[KeyType <: IndexedRecord, ValueType <: IndexedRecord] extends RoutingProtocol[KeyType, ValueType] {
	import net.lag.logging.Logger

	protected def readQuorum:Double
	protected def writeQuorum:Double
	val statslogger = Logger("statsprotocol")
	/**
	* For each range represented by me, get the worklad stats from all the partitions
	* Add stats from partitions that cover the same range, dividing by the quorum amount
	* (Do this to get "logical" workload to a partition)
	* Return mapping of startkey to tuple of (gets,puts)
	* TODO: is this assuming that the read/write quorum will never change?
	*/
	def getWorkloadStats(time:Long):Map[Option[org.apache.avro.generic.GenericData.Record], (Long,Long)] = {
		var haveStats = false
		val ranges = serversForRange(None,None)
		val requests = for (fullrange <- ranges) yield {
			(fullrange.startKey,fullrange.endKey, fullrange.values, for (partition <- fullrange.values) yield {partition !! GetWorkloadStats()} )
		}
		val result = Map( requests.map(range => {
			var gets = 0L; var puts = 0L
			val readDivisor = range._4.size//scala.math.ceil(range._4.size * readQuorum).toLong
			val writeDivisor = range._4.size//scala.math.ceil(range._4.size * writeQuorum).toLong

			range._4.foreach(resp => { val (getcount,putcount,statstime) = resp.get match {
				case r:GetWorkloadStatsResponse => (r.getCount.toLong,r.putCount.toLong,r.statsSince.toLong); case _ => (0L,0L,0L)
				}; gets += getcount; puts += putcount; statslogger.debug("workload stats for: %s",statstime.toString); if (statstime >= time) haveStats = true} )
			(range._1.map(_.toGenericRecord) -> ( gets/readDivisor , puts/writeDivisor ))
		}):_* )

		if (haveStats) result else null
	}
}