package edu.berkeley.cs.scads.storage

import edu.berkeley.cs.scads.comm._

import TxStatus._

sealed case class RecordUpdateInfo(servers: Seq[PartitionService], update: RecordUpdate)

trait ProtocolBase {
  def RunProtocol(tx: Tx)

  protected def transformUpdateList(updateList: UpdateList, readList: ReadList): Seq[RecordUpdateInfo] = {
    updateList.getUpdateList.map(update => {
      update match {
        case VersionUpdateInfo(servers, key, value) => {
          val md = readList.getRecord(key).map(r =>
            MDCCMetadata(r.metadata.currentRound + 1, r.metadata.ballots))
          val newBytes = MDCCRecordUtil.toBytes(value, md)
          RecordUpdateInfo(servers, VersionUpdate(key, newBytes))
        }
        case ValueUpdateInfo(servers, key, value) => {
          val md = readList.getRecord(key).map(r =>
            MDCCMetadata(r.metadata.currentRound, r.metadata.ballots))
          val newBytes = MDCCRecordUtil.toBytes(value, md)
          RecordUpdateInfo(servers, ValueUpdate(key, None, newBytes))
        }
        case LogicalUpdateInfo(servers, key, value) => {
          val md = readList.getRecord(key).map(r =>
            MDCCMetadata(r.metadata.currentRound, r.metadata.ballots))
          val newBytes = MDCCRecordUtil.toBytes(value, md)
          RecordUpdateInfo(servers, LogicalUpdate(key, newBytes))
        }
      }
    })
  }
}
