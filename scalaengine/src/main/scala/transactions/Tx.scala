package edu.berkeley.cs.scads.storage.transactions

import mdcc.MDCCHandler
import net.lag.logging.Logger

import java.lang.Thread
import java.util.Calendar
import prot2pc.Protocol2pc

sealed trait TxStatus { def name: String }
case object UNKNOWN extends TxStatus { val name = "UNKNOWN" }
case object COMMIT extends TxStatus { val name = "COMMIT" }
case object ABORT extends TxStatus { val name = "ABORT" }



sealed trait TxProtocol
case class TxProtocolNone() extends TxProtocol
case class TxProtocol2pc() extends TxProtocol
case class TxProtocolMDCC() extends TxProtocol

class Tx(timeout: Int)(mainFn: => Unit) {
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
    updateList = new UpdateList
    readList = new ReadList
    ThreadLocalStorage.updateList.withValue(Some(updateList)) {
      ThreadLocalStorage.txReadList.withValue(Some(readList)) {
        ThreadLocalStorage.protocolMap.withValue(Some(protocolMap)) {
          mainFn
        }
      }
    }

    protocolMap.getProtocol() match {
      case TxProtocolNone() =>
      case TxProtocol2pc() => Protocol2pc.RunProtocol(this)
      case TxProtocolMDCC() => MDCCHandler.RunProtocol(this)
      case null => throw new RuntimeException("All namespaces in the transaction must have the same protocol.")
    }
  }

}
