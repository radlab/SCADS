package edu.berkeley.cs.scads.storage.transactions

import mdcc.MDCCHandler
import net.lag.logging.Logger

import java.lang.Thread
import java.util.Calendar
import prot2pc.Protocol2pc

import edu.berkeley.cs.avro.marker._

sealed trait TxStatus { def name: String }
case object UNKNOWN extends TxStatus { val name = "UNKNOWN" }
case object COMMITTED extends TxStatus { val name = "COMMIT" }
case object ABORTED extends TxStatus { val name = "ABORT" }

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

class Tx(timeout: Int, readType: ReadConsistency = ReadConsistent())(mainFn: => Unit) {
  var unknownFn = () => {}
  var acceptFn = () => {}
  var commitFn = (status: TxStatus) => {}

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

  def Commit(f: TxStatus => Unit) = {
    commitFn = f
    this
  }

  def Execute() {
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

    protocolMap.getProtocol() match {
      case NSTxProtocolNone() =>
      case NSTxProtocol2pc() => Protocol2pc.RunProtocol(this)
      case NSTxProtocolMDCC() => MDCCHandler.RunProtocol(this)
      case null => throw new RuntimeException("All namespaces in the transaction must have the same protocol.")
    }
    val endMS = java.util.Calendar.getInstance().getTimeInMillis()
    println("latency: " + (endMS - startMS))
    endMS - startMS
  }

}
