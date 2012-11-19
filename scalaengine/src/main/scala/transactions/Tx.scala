package edu.berkeley.cs.scads.storage.transactions

import mdcc.MDCCTrxHandler
import net.lag.logging.Logger

import java.lang.Thread
import java.util.Calendar
import prot2pc.Protocol2pc
import mdcc.MDCCProtocol

import collection.mutable.ArrayBuffer

import edu.berkeley.cs.avro.marker._

sealed trait TxStatus { def name: String }
case object UNKNOWN extends TxStatus { val name = "UNKNOWN" }
case object COMMITTED extends TxStatus { val name = "COMMIT" }
case object ABORTED extends TxStatus { val name = "ABORT" }
case object REJECTED extends TxStatus { val name = "REJECT" }

sealed trait ReadConsistency
case class ReadLocal() extends ReadConsistency
case class ReadCustom(size: Int) extends ReadConsistency
case class ReadConsistent() extends ReadConsistency
case class ReadAll() extends ReadConsistency

// Types of transactions for namespaces.
sealed trait NSTxProtocol extends AvroUnion
case class NSTxProtocolNone() extends AvroRecord with NSTxProtocol
case class NSTxProtocol2pc() extends AvroRecord with NSTxProtocol
case class NSTxProtocolMDCC() extends AvroRecord with NSTxProtocol

class Tx(val timeout: Int, val readType: ReadConsistency = ReadConsistent())(mainFn: => Unit) {
  var unknownFn = () => {}
  var acceptFn = () => {}
  var probFn = (status: Seq[RowLikelihood], prob: Double) => {false}
  var commitFn = (status: TxStatus) => {}
  var finallyFn = (status: TxStatus, timedOut: Boolean, endSpecTime: Long) => {}

  var updateList = new UpdateList
  var readList = new ReadList
  var protocolMap = new ProtocolMap

  def Unknown(f: => Unit) = {
    unknownFn = f _
    this 
  }

  def Accept(p: Double)(f: => Unit) = {
    acceptFn = f _
    this 
  }

  // TODO: call this progress
  def Probabilistic(f: (Seq[RowLikelihood], Double) => Boolean) = {
    probFn = f
    this
  }

  def Commit(f: TxStatus => Unit) = {
    commitFn = f
    this
  }

  def Finally(f: (TxStatus, Boolean, Long) => Unit) = {
    finallyFn = f
    this
  }

  def Execute(): TxStatus = {
    val startMS = java.util.Calendar.getInstance().getTimeInMillis()
    updateList = new UpdateList
    readList = new ReadList
    ThreadLocalStorage.updateList.withValue(Some(updateList)) {
      ThreadLocalStorage.txReadList.withValue(Some(readList)) {
        ThreadLocalStorage.protocolMap.withValue(Some(protocolMap)) {
          ThreadLocalStorage.readConsistency.withValue(readType) {
            mainFn
          }
        }
      }
    }

    val txStatus = protocolMap.getProtocol() match {
      case NSTxProtocolNone() => ProtocolNone.RunProtocol(this)
      case NSTxProtocol2pc() => Protocol2pc.RunProtocol(this)
      case NSTxProtocolMDCC() => MDCCProtocol.RunProtocol(this)
      case null => if (updateList.size == 0) {
        // Read-only transaction.
        COMMITTED
      } else {
        throw new RuntimeException("All namespaces in a write transaction must have the same protocol.")
      }
    }
    val endMS = java.util.Calendar.getInstance().getTimeInMillis()
//    println("latency: " + (endMS - startMS))
    txStatus
  }

}

class RowLikelihood(var initialDesc: String, var initialProb: Double, var initialDuration: Double) {
  // (string description, double probability, double duration)
  val probs = new ArrayBuffer[(String, Double, Double)]
  probs.append((initialDesc, initialProb, initialDuration))

  def append(desc: String, prob: Double, duration: Double) = {
    probs.append((desc, prob, duration))
  }

  def last = probs.last
  def lastProb = probs.last._2

  override def toString() = {
    probs.toString
  }
}

