package edu.berkeley.cs
package scads
package storage

trait DebuggingClient {
  /*
  def iterateOverRange(startKey: Option[KeyType], endKey: Option[KeyType]): Iterator[RangeType] = {
    val partitions = serversForRange(startKey, endKey)
    partitions.map(p => new ActorlessPartitionIterator(p.values.head, p.startKey.map(serializeKey), p.endKey.map(serializeKey)))
	      .foldLeft(Iterator.empty.asInstanceOf[Iterator[Record]])(_ ++ _)
	      .map(rec => extractRangeTypeFromRecord(rec.key, rec.value).get)
  }

  // For debugging only
  def dumpDistribution: Unit = {
    val futures = serversForRange(None, None).flatMap(r =>
      r.values.map(p => p !! CountRangeRequest(r.startKey.map(serializeKey), r.endKey.map(serializeKey))))

    futures.foreach(f => 
      f() match {
        case CountRangeResponse(num) => logger.info("%s: %d", f.source, num)
        case _ => logger.warning("Invalid response from %s", f.source)
      }
    )
  }

  def dumpWorkload: Unit = {
    def futures = serversForRange(None,None).map(r =>
      (r.startKey, r.values.map(p => (p, p !! GetWorkloadStats()))))

    futures.foreach {
      case (startKey, replicas) => {
	logger.info("==%s==", startKey)
	replicas.map(f => (f._1, f._2())).foreach {
	  case (hander, GetWorkloadStatsResponse(getCount, putCount, _)) => logger.info("%s: %d gets, %d puts", getCount, putCount)
	  case m => logger.warning("Invalid message received for workload stats %s", m)
	}
      }
    }
  }
  */
}
