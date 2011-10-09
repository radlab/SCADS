package edu.berkeley.cs.scads.storage.transactions.prot2pc


import _root_.edu.berkeley.cs.scads.storage.transactions._
import edu.berkeley.cs.scads.comm._

import mdcc.MDCCMetaDefault
import net.lag.logging.Logger

import java.lang.Thread
import java.util.Calendar

sealed case class RecordUpdateInfo(servers: Seq[PartitionService], update: RecordUpdate)

object Protocol2pc extends ProtocolBase {
  def RunProtocol(tx: Tx) {
    println("***** 2pc protocol *****")
    val tid = Calendar.getInstance().getTimeInMillis()
    val commitTest = PrepareTest(tx, tid)
    CommitTest(tx, tid, commitTest)
  }

  protected def transformUpdateList(updateList: UpdateList, readList: ReadList): Seq[RecordUpdateInfo] = {
    updateList.getUpdateList.map(update => {
      update match {
        case ValueUpdateInfo(ns, servers, key, value) => {
          val md = readList.getRecord(key).map(r =>
            MDCCMetadata(r.metadata.currentRound, r.metadata.ballots)) match {
              case None => Some(ns.getDefaultMeta())
              case x => x
            }
          //TODO: Do we really need the MDCCMetadata
          val newBytes = MDCCRecordUtil.toBytes(value, md)
          RecordUpdateInfo(servers, ValueUpdate(key, None, newBytes))
        }
        case LogicalUpdateInfo(ns, servers, key, value) => {
          val md = readList.getRecord(key).map(r =>
            MDCCMetadata(r.metadata.currentRound, r.metadata.ballots)) match {
              case None => Some(ns.getDefaultMeta())
              case x => x
            }
          val newBytes = MDCCRecordUtil.toBytes(value, md)
          RecordUpdateInfo(servers, LogicalUpdate(key, newBytes))
        }
      }
    })
  }

  def PrepareTest(tx: Tx, tid: Long) = {
    var count = 0

    val responses = transformUpdateList(tx.updateList, tx.readList).map(t => {
      val servers = t.servers
      val recordUpdate = t.update
      val putRequest = PrepareRequest(ScadsXid(tid, count),
                                      List(recordUpdate))
      count += 1
      (servers.map(_ !! putRequest), servers.length)
    })

    val results = responses.map(x => {
      val res = x._1.blockFor(x._2).map(f => f() match {
        case PrepareResponse(success) => success
        case m => false
      })
      res
    })

    val commitTest = !results.map(x => !x.contains(false)).contains(false)
    commitTest
  }

  def CommitTest(tx: Tx, tid: Long, commitTest: Boolean) {
    var count = 0
    val commitResponses = transformUpdateList(tx.updateList, tx.readList).map(t => {
      val servers = t.servers
      val recordUpdate = t.update
      val commitRequest = CommitRequest(ScadsXid(tid, count),
                                        List(recordUpdate),
                                        commitTest)
      count += 1
      (servers.map(_ !! commitRequest), servers.length)
    })
    commitResponses.foreach(x => x._1.blockFor(x._2))
  }
}
