package edu.berkeley.cs.scads.storage.transactions

import _root_.edu.berkeley.cs.scads.comm._
import _root_.edu.berkeley.cs.scads.comm.CommitResponse._
import _root_.edu.berkeley.cs.scads.comm.PrepareResponse._
import _root_.edu.berkeley.cs.scads.storage.StorageManager


abstract class TrxManager() {
  def process(src: Option[RemoteActorProxy], msg : TrxMessage) (implicit sender: RemoteActorProxy)


}

//TODO factor 2PC prot
class Protocol2PCManager(manager: StorageManager)  extends TrxManager {

   def process(src: Option[RemoteActorProxy], msg : TrxMessage)(implicit sender: RemoteActorProxy) = {
     def reply(body: MessageBody) = src.foreach(_ ! body)
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

