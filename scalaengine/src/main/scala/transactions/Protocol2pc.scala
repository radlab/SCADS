package edu.berkeley.cs.scads.storage
package transactions
package prot2pc

import net.lag.logging.Logger
import java.util.Calendar

object Protocol2pc extends ProtocolBase {
  def RunProtocol(tx: Tx) {
    val xid = ScadsXid.createUniqueXid
    val updateList = transformUpdateList(tx.updateList, tx.readList)
    if (updateList.size > 0) {
      val commitTest = PrepareTest(tx, xid, updateList)
      CommitTest(tx, xid, updateList, commitTest)
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
      val res = x._1.blockFor(x._2).map(f => f() match {
        case PrepareResponse(success) => success
        case m => false
      })
      res
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
      (servers.map(_ !! commitRequest), servers.length)
    })
    commitResponses.foreach(x => x._1.blockFor(x._2))
  }
}
