package edu.berkeley.cs.scads
package storage
package transactions

import comm._

abstract class TrxManager(manager: StorageManager) {
  def process(src: Option[RemoteServiceProxy[StorageMessage]], msg : TrxMessage) (implicit sender: RemoteServiceProxy[StorageMessage])

}

class Protocol2PCManager(manager: StorageManager)  extends TrxManager(manager) {

   def process(src: Option[RemoteServiceProxy[StorageMessage]], msg : TrxMessage)(implicit sender: RemoteServiceProxy[StorageMessage]) = {
     def reply(body: StorageMessage) = src.foreach(_ ! body)
     msg match {
       case PrepareRequest(xid, updates) => {
         val success = manager.accept(xid, updates).isDefined
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

