package edu.berkeley.cs.scads
package storage
package transactions

import comm._
import conflict._
import MDCCMetaHelper._

class ProtocolMDCCServer(manager: StorageManager, pendingUpdates: PendingUpdates, serverAddress: RemoteServiceProxy[StorageMessage]) extends TrxManager(manager) {


  def getMeta: MDCCMetadata  = null

  def processPropose(src: Option[RemoteServiceProxy[StorageMessage]], xid: ScadsXid, update: RecordUpdate)(implicit sender: RemoteServiceProxy[StorageMessage])  = {
    val meta = pendingUpdates.getMeta(update.key)
    val master = getMaster(meta)
    val result = pendingUpdates.accept(xid, update)
  }

  def processPhase1a(src: Option[RemoteServiceProxy[StorageMessage]], key: Array[Byte], ballot: MDCCMetadata) = {
    val meta = pendingUpdates.getMeta(key)


  }

  def processPhase2a(src: Option[RemoteServiceProxy[StorageMessage]], key: Array[Byte], ballot: MDCCBallot, value: CStruct) = {

  }

  def processAccept(src: Option[RemoteServiceProxy[StorageMessage]], xid: ScadsXid) = {

  }


  def process(src: Option[RemoteServiceProxy[StorageMessage]], msg: TrxMessage)(implicit sender: RemoteServiceProxy[StorageMessage]) = {
    msg match {
      case Propose(xid: ScadsXid, update: RecordUpdate) => processPropose(src, xid, update)
      case Phase1a(key: Array[Byte], ballot: MDCCBallotRange) => processPhase1a(src, key, ballot)
      case Phase2a(key: Array[Byte], ballot: MDCCBallot, value: CStruct) => processPhase2a(src, key, ballot, value)
      case Accept(xid: ScadsXid) =>
      case _ => src.map(_ ! ProcessingException("Trx Message Not Implemented", ""))
    }
  }
}