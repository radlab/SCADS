package edu.berkeley.cs.scads.storage.transactions

import _root_.edu.berkeley.cs.scads.comm._
import _root_.edu.berkeley.cs.scads.comm.CommitResponse._
import _root_.edu.berkeley.cs.scads.comm.PrepareResponse._
import _root_.edu.berkeley.cs.scads.comm.ProcessingException._
import _root_.edu.berkeley.cs.scads.storage.StorageManager
import _root_.edu.berkeley.cs.scads.storage.transactions.conflict.PendingUpdates
import _root_.edu.berkeley.cs.scads.storage.transactions.MDCCMetaHelper._


/**
 * Created by IntelliJ IDEA.
 * User: tim
 * Date: 9/21/11
 * Time: 4:29 PM
 * To change this template use File | Settings | File Templates.
 */

class ProtocolMDCCServer(manager: StorageManager, pendingUpdates: PendingUpdates, serverAddress: RemoteActorProxy) extends TrxManager(manager) {


  def getMeta: MDCCMetadata  = null

  def processPropose(src: Option[RemoteActorProxy], xid: ScadsXid, update: RecordUpdate)(implicit sender: RemoteActorProxy)  = {
    pendingUpdates.getMeta(update.key) match {
      case None => src.map(_ ! ProcessingException("Inserts are not yet implemented", ""))
      case Some(meta) => {
        val master = getMaster(meta)
        val result = pendingUpdates.accept(xid, update)
        throw new RuntimeException("check implementation")
        //src.map(_ !! Phase2bFast(meta.currentRound, result.map(_._2)))
      }
    }
  }

  def processPhase1a(src: Option[RemoteActorProxy], key: Array[Byte], ballot: MDCCBallotRange) = {



  }

  def processPhase2a(src: Option[RemoteActorProxy], key: Array[Byte], ballot: MDCCBallot, value: CStruct) = {

  }

  def processAccept(src: Option[RemoteActorProxy], xid: ScadsXid) = {

  }


  def process(src: Option[RemoteActorProxy], msg: TrxMessage)(implicit sender: RemoteActorProxy) = {
    msg match {
      case Propose(xid: ScadsXid, update: RecordUpdate) => processPropose(src, xid, update)
      case Phase1a(key: Array[Byte], ballot: MDCCBallotRange) => processPhase1a(src, key, ballot)
      case Phase2a(key: Array[Byte], ballot: MDCCBallot, value: CStruct) => processPhase2a(src, key, ballot, value)
      case Accept(xid: ScadsXid) =>
      case _ => src.map(_ ! ProcessingException("Trx Message Not Implemented", ""))
    }
  }
}