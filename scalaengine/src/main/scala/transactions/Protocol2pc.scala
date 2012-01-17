package edu.berkeley.cs.scads.storage
package transactions
package prot2pc

import net.lag.logging.Logger
import java.util.Calendar
import java.util.concurrent.TimeUnit

object Protocol2pc extends ProtocolBase {
  def RunProtocol(tx: Tx): TxStatus = {
    val xid = ScadsXid.createUniqueXid
    val updateList = transformUpdateList(tx.updateList, tx.readList)
    if (updateList.size > 0) {
      val commitTest = PrepareTest(tx, xid, updateList)
      CommitTest(tx, xid, updateList, commitTest)
      if (commitTest) {
        COMMITTED
      } else {
        ABORTED
      }
    } else {
      COMMITTED
    }
  }

  def PrepareTest(tx: Tx, xid: ScadsXid, updateList: Seq[RecordUpdateInfo]) = {
    val responses = updateList.map(t => {
      val servers = t.servers
      val recordUpdate = t.update
      val putRequest = PrepareRequest(xid,
                                      List(recordUpdate))
      (servers.map(_ !! putRequest), servers.length)
    })

    val results = responses.map(x => {
      try {
        val res = x._1.blockFor(x._2, 5000, TimeUnit.MILLISECONDS).map(f => f() match {
          case PrepareResponse(success) => success
          case m => false
        })
        res
      } catch {
        case _ => List(false)
      }
    })

    val commitTest = !results.map(x => !x.contains(false)).contains(false)
    commitTest
  }

  def CommitTest(tx: Tx, xid: ScadsXid, updateList: Seq[RecordUpdateInfo],
                 commitTest: Boolean) {
    val commitResponses = updateList.map(t => {
      val servers = t.servers
      val recordUpdate = t.update
      val commitRequest = CommitRequest(xid,
                                        List(recordUpdate),
                                        commitTest)
      (servers.map(_ !! commitRequest), commitRequest, servers)
    })
    commitResponses.foreach(x => {
      var success = true
      var futures = x._1
      do {
        try {
          futures.blockFor(x._3.length, 5000, TimeUnit.MILLISECONDS)
          success = true
        } catch {
          case _ => {
            futures = x._3.map(_ !! x._2)
            success = false
          }
        }
      } while (!success)
    })
  }
}
