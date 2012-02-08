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
                  ) : MDCCRecordHandler = getOrCreate(key, value, mt, servers, conflictResolver, remoteHandle)

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

final class ByteArrayWrapper(val value : Array[Byte]) {

    def apply(value : Array[Byte])  = new  ByteArrayWrapper(value)

    override def equals(other : Any) : Boolean =  {
       if(other.isInstanceOf[ByteArrayWrapper]){
         java.util.Arrays.equals(value, other.asInstanceOf[ByteArrayWrapper].value)
       } else{
         return false
       }
    }

    override def hashCode() : Int = java.util.Arrays.hashCode(value)
}


class MDCCRecordCache() {

  private val logger = Logger(classOf[MDCCRecordCache])

  val CACHE_SIZE = 1000000

  def killHandler (key : ByteArrayWrapper, handler : MDCCRecordHandler) = handler.kill

  //TODO: If we wanna use the cache for reads, we should use a lock-free structure
  lazy val cache = new LRUMap[ByteArrayWrapper, MDCCRecordHandler](CACHE_SIZE, None, killHandler){
      protected override def canExpire(k: ByteArrayWrapper, v: MDCCRecordHandler): Boolean = {
        v.getStatus == READY && v.mailbox.size == 0
      }
    }

  def get(key : Array[Byte]) : Option[MDCCRecordHandler] = {
    cache.synchronized{
      cache.get(new ByteArrayWrapper(key))
    }
  }




  def getOrCreate(key : Array[Byte],
                  value : CStruct,
                  mt: MDCCMetadata,
                  servers: Seq[PartitionService],
                  conflictResolver : ConflictResolver,
                  master : SCADSService
                  ) : MDCCRecordHandler = {
    val keyWrapper =  new ByteArrayWrapper(key)
    cache.synchronized{
      cache.get(keyWrapper) match {
        case None => {
          logger.debug("Hash " + hashCode() + ":" + cache.elements.map(_._1.hashCode()).mkString)
          var handler = new MDCCRecordHandler(key, value, mt.currentVersion, mt.ballots,  mt.confirmedBallot, servers, conflictResolver, master)
          cache.update(keyWrapper, handler)
          logger.debug("No record handler exists, we create a new one: hash: %s remote: %s", handler.hashCode(), handler.remoteHandle.id)
          handler
        }
        case Some(v) => {
          logger.debug("We found a record handler and return it. hash: %s remote: %s", v.hashCode(), v.remoteHandle.id)
          v
        }
      }
    }
  }

}
