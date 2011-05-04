package edu.berkeley.cs
package scads
package storage

import comm._
import org.apache.avro.generic.IndexedRecord

trait DebuggingClient {
  self: Namespace
    with KeyRangeRoutable
    with ParFuture
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

    val counts = futures.map(f =>
      f() match {
        case CountRangeResponse(num) => num
        case _ => 0
      }
    )

    logger.info("Max Skew: %d, %f", counts.max - counts.min, (counts.max - counts.min).toFloat / counts.max)
  }

  def dumpWorkload: Unit = {
    val partitions = serversForKeyRange(None, None)
    logger.info("\nStats for namespace %s: %d partitions", namespace, partitions.size)
    val futures = partitions.map(p =>
      (p.startKey, p.servers.map(s => (s, s !! GetWorkloadStats()))))


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

  //HACK
  def rebalance: Unit = {
    val partitions = serversForKeyRange(None, None)
    val distribution = waitForAndThrowException(partitions.flatMap(p => p.servers.map(s => (s !! CountRangeRequest(p.startKey, p.endKey), (s, p.startKey.map(bytesToKey), p.endKey.map(bytesToKey)))))) {
      case (CountRangeResponse(c), p) => (c, p) }
    logger.debug("==distribution==")
    distribution.foreach(logger.debug("%s: %s", namespace, _))

    val workload = waitForAndThrowException(partitions.flatMap(p => p.servers.map(s => (s !! GetWorkloadStats(), (s, p.startKey.map(bytesToKey), p.endKey.map(bytesToKey)))))) {
      case (GetWorkloadStatsResponse(gets, puts, _), p) => (gets, puts, p) }
    logger.debug("==workload==")
    workload.foreach(logger.debug("%s: %s", namespace, _))

    val counts = distribution.map(_._1)
    val total = counts.sum
    val desiredKeysPerServer = total / partitions.size
    logger.debug("%s: desiredKeysPerServer: %d", namespace, desiredKeysPerServer)
  }
}
