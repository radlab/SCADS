package edu.berkeley.cs.scads.storage

import _root_.edu.berkeley.cs.scads.comm._
import _root_.edu.berkeley.cs.scads.comm.CommitResponse._
import _root_.edu.berkeley.cs.scads.comm.PrepareResponse._
import _root_.edu.berkeley.cs.scads.storage.StorageManager


abstract class TrxManager(manager: StorageManager) {
  def process(src: Option[RemoteActorProxy], msg : TrxMessage) (implicit sender: RemoteActorProxy)

}

class Protocol2PC(manager: StorageManager)  extends TrxManager(manager) {

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
     }
   }
}

