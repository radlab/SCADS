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
      case TxProtocolMDCC() => ProtocolMDCC.RunProtocol(this)
      case null => throw new RuntimeException("All namespaces in the transaction must have the same protocol.")
    }
  }

}
