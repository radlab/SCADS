package edu.berkeley.cs.scads
package storage
package transactions
package mdcc

import conflict.ConflictResolver
import storage.StorageEnvelope
import comm.{Envelope, Mailbox}
import util.Logger
import util.LRUMap

import java.util.concurrent.ConcurrentHashMap

class MDCCClientServer(ns : TransactionI) extends  MDCCRecordCache {
   private implicit val remoteHandle = StorageService(StorageRegistry.registerFastMailboxFunc(processMailbox))

   private val logger = Logger(classOf[MDCCClientServer])

   def forwardToRecordHandler(key : Array[Byte], env : Envelope[StorageMessage]) = {
     val servers = ns.serversForKey(key)
     val handler = getOrCreate(key, CStruct(None, Nil), ns.getDefaultMeta(key), servers, ns.getConflictResolver, remoteHandle)
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
        case env@StorageEnvelope(src, BeMaster(key, _, _, _, _)) => forwardToRecordHandler(key, env)
        case env@StorageEnvelope(src, ResolveConflict(key, _, _, _)) =>  forwardToRecordHandler(key, env)
        case env@StorageEnvelope(src, SinglePropose(_, update, _)) =>  forwardToRecordHandler(update.key, env)
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

  val handlerMap = new ConcurrentHashMap[ByteArrayWrapper, MDCCRecordHandler](CACHE_SIZE, 0.75f, 100)

  def get(key : Array[Byte]) : Option[MDCCRecordHandler] = {
    Option(handlerMap.get(new ByteArrayWrapper(key)))
  }

  def getOrCreate(key : Array[Byte],
                  value : CStruct,
                  mt: MDCCMetadata,
                  servers: Seq[PartitionService],
                  conflictResolver : ConflictResolver,
                  master : SCADSService
                  ) : MDCCRecordHandler = {
    val startT = System.nanoTime / 1000000
    val keyWrapper =  new ByteArrayWrapper(key)
    val endT2 = System.nanoTime / 1000000
    handlerMap.get(keyWrapper) match {
      case null => {
        val endT3 = System.nanoTime / 1000000
        var handler = new MDCCRecordHandler(key, value, mt.currentVersion, mt.ballots,  mt.confirmedBallot, servers, conflictResolver, master)
        val endT4 = System.nanoTime / 1000000
        val oldVal = handlerMap.putIfAbsent(keyWrapper, handler)
        if (oldVal != null) handler = oldVal
        logger.debug("No record handler exists, we create a new one: hash: %s remote: %s", handler.hashCode(), handler.remoteHandle.id)
        val endT = System.nanoTime / 1000000
        if (endT - startT > 10) {
//          logger.error("slow %s [%s, %s, %s, %s] getOrCreate1: %s", Thread.currentThread.getName, (endT2 - startT), (endT3 - endT2), (endT4 - endT3), (endT - endT4), (endT - startT))
        }
        handler
      }
      case v => {
        val endT = System.nanoTime / 1000000
        if (endT - startT > 10) {
//          logger.error("slow %s [%s, %s] getOrCreate2: %s", Thread.currentThread.getName, (endT2 - startT), (endT - endT2), (endT - startT))
        }
        logger.debug("We found a record handler and return it. hash: %s remote: %s", v.hashCode(), v.remoteHandle.id)
        v
      }
    }
  }

/*
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
    val startT = System.nanoTime / 1000000
    val keyWrapper =  new ByteArrayWrapper(key)
    cache.synchronized{
      val endT2 = System.nanoTime / 1000000
      cache.get(keyWrapper) match {
        case None => {
          val endT3 = System.nanoTime / 1000000
          var handler = new MDCCRecordHandler(key, value, mt.currentVersion, mt.ballots,  mt.confirmedBallot, servers, conflictResolver, master)
          val endT4 = System.nanoTime / 1000000
          cache.update(keyWrapper, handler)
          logger.debug("No record handler exists, we create a new one: hash: %s remote: %s", handler.hashCode(), handler.remoteHandle.id)
          val endT = System.nanoTime / 1000000
          if (endT - startT > 20) {
            logger.error("slow %s [%s, %s, %s, %s] getOrCreate1: %s", Thread.currentThread.getName, (endT2 - startT), (endT3 - endT2), (endT4 - endT3), (endT - endT4), (endT - startT))
          }
          handler
        }
        case Some(v) => {
          val endT = System.nanoTime / 1000000
          if (endT - startT > 20) {
            logger.error("slow %s [%s, %s] getOrCreate2: %s", Thread.currentThread.getName, (endT2 - startT), (endT - endT2), (endT - startT))
          }
          logger.debug("We found a record handler and return it. hash: %s remote: %s", v.hashCode(), v.remoteHandle.id)
          v
        }
      }
    }
  }
*/

}
