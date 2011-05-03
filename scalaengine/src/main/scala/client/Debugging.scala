package edu.berkeley.cs
package scads
package storage

import comm._
import org.apache.avro.generic.IndexedRecord

trait DebuggingClient {
  self: Namespace
    with KeyRangeRoutable
    with Serializer[IndexedRecord, IndexedRecord, _] =>

  /*
  def iterateOverRange(startKey: Option[KeyType], endKey: Option[KeyType]): Iterator[RangeType] = {
    val partitions = serversForKeyRange(startKey, endKey)
    partitions.map(p => new ActorlessPartitionIterator(p.values.head, p.startKey, p.endKey
	      .foldLeft(Iterator.empty.asInstanceOf[Iterator[Record]])(_ ++ _)
	      .map(rec => extractRangeTypeFromRecord(rec.key, rec.value).get)
  }
  */

  // For debugging only
  def dumpDistribution: Unit = {
    logger.info("\nKey Distribution for %s", namespace)
    val futures = serversForKeyRange(None, None).flatMap(r =>
      r.servers.map(p => p !! CountRangeRequest(r.startKey, r.endKey)))

    futures.foreach(f =>
      f() match {
        case CountRangeResponse(num) => logger.info("%s: %d", f.source, num)
        case _ => logger.warning("Invalid response from %s", f.source)
      }
    )
  }

  def dumpWorkload: Unit = {
    val partitions = serversForKeyRange(None, None)
    logger.info("\nStats for namespace %s: %d partitions", namespace, partitions.size)
    val futures = partitions.map(r =>
      (r.startKey, r.servers.map(p => (p, p !! GetWorkloadStats()))))


    futures.foreach {
      case (startKey, replicas) => {
        logger.info("==%s==", startKey.map(bytesToKey))
        replicas.map(f => (f._1, f._2())).foreach {
          case (hander, GetWorkloadStatsResponse(getCount, putCount, _)) => logger.info("%s: %d gets, %d puts", hander, getCount, putCount)
          case m => logger.warning("Invalid message received for workload stats %s", m)
        }
      }
    }

    val values =
      futures.flatMap {
        case (startKey, replicas) => {
          replicas.map(f => (f._1, f._2())).map {
            case (hander, GetWorkloadStatsResponse(x, y, _)) => (x, y)
            case m => {
              logger.warning("Invalid message received for workload stats %s", m)
              (0, 0)
            }
          }
        }
      }

    logger.info("getDiff: %d, putDiff: %d\n", values.map(_._1).max - values.map(_._1).min, values.map(_._2).max - values.map(_._2).min)
  }
}
