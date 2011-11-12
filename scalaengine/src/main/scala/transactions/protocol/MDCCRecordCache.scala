package edu.berkeley.cs.scads
package storage
package transactions
package mdcc

import util.LRUMap
import conflict.ConflictResolver

class MDCCRecordCache() {

  val CACHE_SIZE = 500

  def killHandler (key : Array[Byte], handler :  MCCCRecordHandler) = handler.kill

  //TODO: If we wanna use the cache for reads, we should use a lock-free structure
  lazy val cache = new LRUMap[Array[Byte], MCCCRecordHandler](CACHE_SIZE, None, killHandler){
      protected override def canExpire(k: Array[Byte], v: MCCCRecordHandler): Boolean = v.getStatus == READY
    }

  def get(key : Array[Byte]) : Option[MCCCRecordHandler] = {
    cache.synchronized{
      cache.get(key)
    }
  }

  def getOrCreate(key : Array[Byte],
                  value : CStruct,
                  servers: Seq[PartitionService],
                  mt: MDCCMetadata,
                  conflictResolver : ConflictResolver,
                  confirmedBallot : Boolean = false,
                  confirmedVersion : Boolean = false
                  ) : MCCCRecordHandler = {
    cache.synchronized{
      cache.get(key) match {
        case None => {
          var handler = new MCCCRecordHandler(key, value, servers, mt.currentVersion, mt.ballots, confirmedBallot, confirmedVersion, conflictResolver)
          cache.update(key, handler)
          handler
        }
        case Some(v) => v
      }
    }
  }

}
