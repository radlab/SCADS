package edu.berkeley.cs.scads.storage.transactions

import edu.berkeley.cs.scads.comm._
import edu.berkeley.cs.scads.storage._

import java.util.concurrent._

sealed case class RecordUpdateInfo(servers: Seq[PartitionService], update: RecordUpdate)

trait ProtocolBase {
  def RunProtocol(tx: Tx): TxStatus

  protected def transformUpdateList(updateList: UpdateList, readList: ReadList): Seq[RecordUpdateInfo] = {
    updateList.getUpdateList.map(update => {
      update match {
        case ValueUpdateInfo(ns, servers, key, value) => {
          val (md, oldBytes) = readList.getRecord(key) match {
            case None => (ns.getDefaultMeta(), None)
            case Some(r) => (r.metadata, Some(MDCCRecordUtil.toBytes(r)))
          }
          //TODO: Do we really need the MDCCMetadata
          val newBytes = MDCCRecordUtil.toBytes(value, md)
          RecordUpdateInfo(servers, ValueUpdate(key, oldBytes, newBytes))
        }
        case LogicalUpdateInfo(ns, servers, key, value) => {
          val md = readList.getRecord(key) match {
            case None => ns.getDefaultMeta()
            case Some(r) => r.metadata
          }
          val newBytes = MDCCRecordUtil.toBytes(value, md)
          RecordUpdateInfo(servers, LogicalUpdate(key, newBytes))
        }
      }
    })
  }

}

object ProtocolNone extends ProtocolBase {
  def RunProtocol(tx: Tx): TxStatus = {
    val responses = transformUpdateList(tx.updateList, tx.readList).map(t => {
      val servers = t.servers
      val recordUpdate = t.update

      recordUpdate match {
        case ValueUpdate(key, _, newBytes) => {
          val putRequest = PutRequest(key, Some(newBytes))
          (servers.map(_ !! putRequest), servers.length)
        }
        case _ => throw new RuntimeException("ProtocolNone can only run ValueUpdates.")
      }
    })

    responses.foreach(t =>
      t._1.blockFor(writeQuorum(t._2), 5000, TimeUnit.MILLISECONDS))
    COMMITTED
  }

  private def writeQuorum(numServers: Int): Int = {
    scala.math.ceil(numServers * 0.501).toInt
  }
}
