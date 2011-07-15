package edu.berkeley.cs.scads.storage

import edu.berkeley.cs.scads.comm._

import net.lag.logging.Logger

import java.lang.Thread
import javax.transaction.xa._
import java.nio.ByteBuffer
import java.util.Calendar

object TxStatus extends Enumeration {
  type TxStatus = Value
  val SUCCESS, FAILURE = Value
}
import TxStatus._

class ScadsXid(val tid: Long, val bid: Long) extends Xid {
  def getBranchQualifier(): Array[Byte] = {
    ByteBuffer.allocate(64).putLong(bid).array
  }

  def getFormatId(): Int = {
    0
  }

  def getGlobalTransactionId(): Array[Byte] = {
    ByteBuffer.allocate(64).putLong(tid).array
  }

  def serialized(): Array[Byte] = {
    // Long is 8 bytes
    ByteBuffer.allocate(16).putLong(tid).putLong(bid).array
  }
}

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
    ThreadLocalStorage.updateList.withValue(Some(updateList)) {
      mainFn
    }
    RunProtocol()
  }


  def ExecuteMain() {
    ThreadLocalStorage.updateList.withValue(Some(updateList)) {
      mainFn
    }
  }

  private lazy val tid = Calendar.getInstance().getTimeInMillis()
  private var commit2 = true
  def PrepareTest() {
    var count = 0

    val responses = updateList.updateList.readOnly.map(t => {
      val servers = t._1
      val key = t._2
      val value = t._3
      val putRequest = PrepareRequest(tid, count, List(PutRequest(key, value)))
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

    val commit = !results.map(x => !x.contains(false)).contains(false)
    println("commit: " + commit)

  
    commit2 = commit
  }

  def CommitTest() {
    var count = 0
    val commitResponses = updateList.updateList.readOnly.map(t => {
      val servers = t._1
      val key = t._2
      val value = t._3
      val commitRequest = CommitRequest(tid, count, commit2)
      count += 1
      (servers.map(_ !! commitRequest), servers.length)
    })
    commitResponses.foreach(x => x._1.blockFor(x._2))
  }



  // just blocking 2pc
  def RunProtocol() {
    val tid = Calendar.getInstance().getTimeInMillis()
    var count = 0

    val responses = updateList.updateList.readOnly.map(t => {
      val servers = t._1
      val key = t._2
      val value = t._3
      val putRequest = PrepareRequest(tid, count, List(PutRequest(key, value)))
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

    val commit = !results.map(x => !x.contains(false)).contains(false)
    println("commit: " + commit)

    count = 0
    val commitResponses = updateList.updateList.readOnly.map(t => {
      val servers = t._1
      val key = t._2
      val value = t._3
      val commitRequest = CommitRequest(tid, count, commit)
      count += 1
      (servers.map(_ !! commitRequest), servers.length)
    })
    commitResponses.foreach(x => x._1.blockFor(x._2))

  }

}

