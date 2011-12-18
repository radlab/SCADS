package edu.berkeley.cs.scads
package storage
package transactions
package mdcc

import conflict.ConflictResolver
import storage.StorageEnvelope
import comm.{Envelope, Mailbox}
import util.Logger
import util.LRUMap

class MDCCClientServer(ns : TransactionI) extends  MDCCRecordCache {
   private implicit val remoteHandle = StorageService(StorageRegistry.registerFastMailboxFunc(processMailbox))

   private val logger = Logger(classOf[MDCCClientServer])

   def forwardToRecordHandler(key : Array[Byte], env : Envelope[StorageMessage]) = {
     val servers = ns.serversForKey(key)
     val handler = getOrCreate(key, CStruct(None, Nil), ns.getDefaultMeta(), servers, ns.getConflictResolver, remoteHandle)
     handler.forwardRequest(env)
   }

   def getOrCreate(key : Array[Byte],
                  value : CStruct,
                  mt: MDCCMetadata,
                  servers: Seq[PartitionService],
                  conflictResolver : ConflictResolver
                  ) : MCCCRecordHandler = getOrCreate(key, value, mt, servers, conflictResolver, remoteHandle)

   def processMailbox(mailbox : Mailbox[StorageMessage]) {
      mailbox{
        case env@StorageEnvelope(src, BeMaster(key, _, _, _)) => forwardToRecordHandler(key, env)
        case env@StorageEnvelope(src, ResolveConflict(key, _, _, _)) =>  forwardToRecordHandler(key, env)
        case env@StorageEnvelope(src, SinglePropose(_, update)) =>  forwardToRecordHandler(update.key, env)
        case env@StorageEnvelope(src, MultiPropose(proposes))  =>  {
          assert(proposes.map(_.update.key).distinct.size == 1, "Currently Multi-Proposes have to contain 1 key")
          forwardToRecordHandler(proposes.head.update.key, env)
        }
        case env@_ =>  logger.error("Got Message to the master without knowing what to do with it", env)

      }
   }
}

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
                  mt: MDCCMetadata,
                  servers: Seq[PartitionService],
                  conflictResolver : ConflictResolver,
                  master : SCADSService
                  ) : MCCCRecordHandler = {
    cache.synchronized{
      cache.get(key) match {
        case None => {
          var handler = new MCCCRecordHandler(key, value, mt.currentVersion, mt.ballots,  mt.confirmedBallot, servers, conflictResolver, master)
          cache.update(key, handler)
          handler
        }
        case Some(v) => v
      }
    }
  }

}
