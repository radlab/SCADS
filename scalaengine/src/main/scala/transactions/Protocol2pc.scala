package edu.berkeley.cs.scads.storage
package transactions
package prot2pc

import net.lag.logging.Logger
import java.util.Calendar

object Protocol2pc extends ProtocolBase {
  def RunProtocol(tx: Tx) {
    println("***** 2pc protocol *****")
    val tid = Calendar.getInstance().getTimeInMillis()
    val commitTest = PrepareTest(tx, tid)
    CommitTest(tx, tid, commitTest)
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
