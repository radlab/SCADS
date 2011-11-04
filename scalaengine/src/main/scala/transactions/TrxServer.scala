package edu.berkeley.cs.scads
package storage
package transactions

import comm._
import conflict._

abstract class TrxManager() {
  def process(src: RemoteServiceProxy[StorageMessage], msg : TrxMessage)

  def shutdown() = {}
}

//TODO factor 2PC prot
class Protocol2PCManager(val pu: PendingUpdates, val manager: StorageManager, implicit val partition : PartitionService)  extends TrxManager {

  def process(src: RemoteServiceProxy[StorageMessage], msg : TrxMessage) = {
    def reply(body: StorageMessage) = src ! body
    msg match {
      case PrepareRequest(xid, updates) => {
        val success = pu.acceptOptionTxn(xid, updates)._1
        reply(PrepareResponse(success))
      }
      case CommitRequest(xid, updates, commit) => {
        var success = true
        if (commit) {
          success = pu.commit(xid, updates)
        } else {
          // Abort
          pu.abortTxn(xid)
        }
        reply(CommitResponse(success))
      }
      case _ => reply(ProcessingException("Trx Message Not Implemented", ""))
    }
  }

  override def shutdown() = {
    pu.shutdown
  }
}
