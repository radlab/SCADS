package edu.berkeley.cs.scads
package storage
package transactions

import comm._

abstract class TrxManager() {
  def process(src: RemoteServiceProxy[StorageMessage], msg : TrxMessage)
}

//TODO factor 2PC prot
class Protocol2PCManager(val manager: StorageManager, implicit val partition : PartitionService)  extends TrxManager {

   def process(src: RemoteServiceProxy[StorageMessage], msg : TrxMessage) = {
     def reply(body: StorageMessage) = src ! body
     msg match {
       case PrepareRequest(xid, updates) => {
         val success = manager.accept(xid, updates)._1
         reply(PrepareResponse(success))
       }
       case CommitRequest(xid, updates, commit) => {
         var success = true
         if (commit) {
           success = manager.commit(xid, updates)
         } else {
           // Abort
           manager.abort(xid)
         }
         reply(CommitResponse(success))
       }
       case _ => reply(ProcessingException("Trx Message Not Implemented", ""))
     }
   }
}

