package edu.berkeley.cs.scads.storage

import edu.berkeley.cs.scads.comm._

import net.lag.logging.Logger

import java.lang.Thread
import java.util.Calendar

object TxStatus extends Enumeration {
  type TxStatus = Value
  val SUCCESS, FAILURE = Value
}
import TxStatus._

// TODO: This is a proof of concept for testing.
//       There should probably be a new trait for each tx protocol 
//       (pnuts, mdcc, 2pc, ...)
class Tx(timeout: Int)(mainFn: => Unit) {
  type FN = Unit => Unit

  var unknownFn = () => {}
  var acceptFn = () => {}
  var commitFn = (status: TxStatus) => {}

  var updateList = new UpdateList

  def Unknown(f: => Unit) = {
    unknownFn = f _
    this 
  }

  def Accept(p: Double)(f: => Unit) = {
    acceptFn = f _
    this 
  }

  def Commit(f: TxStatus => Unit) = {
    commitFn = f
    this
  }

  def Execute() {
    updateList = new UpdateList
    ThreadLocalStorage.updateList.withValue(Some(updateList)) {
      mainFn
    }
    RunProtocol()
  }


  def ExecuteMain() {
    updateList = new UpdateList
    ThreadLocalStorage.updateList.withValue(Some(updateList)) {
      mainFn
    }
  }

  private lazy val tid = Calendar.getInstance().getTimeInMillis()
  private var commitTest = true

  def PrepareTest() {
    var count = 0

    val responses = updateList.updateList.readOnly.map(t => {
      val servers = t._1
      val recordUpdate = t._2
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

    commitTest = !results.map(x => !x.contains(false)).contains(false)
    println("commitTest: " + commitTest)
  }

  def CommitTest() {
    var count = 0
    val commitResponses = updateList.updateList.readOnly.map(t => {
      val servers = t._1
      val recordUpdate = t._2
      val commitRequest = CommitRequest(ScadsXid(tid, count),
                                        List(recordUpdate),
                                        commitTest)
      count += 1
      (servers.map(_ !! commitRequest), servers.length)
    })
    commitResponses.foreach(x => x._1.blockFor(x._2))
  }

  // just blocking 2pc
  def RunProtocol() {
    PrepareTest()
    CommitTest()
  }
}
