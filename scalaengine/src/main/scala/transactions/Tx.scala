package edu.berkeley.cs.scads.storage.transactions

import mdcc.MDCCTrxHandler
import net.lag.logging.Logger

import java.lang.Thread
import java.util.Calendar
import prot2pc.Protocol2pc
import mdcc.MDCCProtocol

import edu.berkeley.cs.avro.marker._

import edu.berkeley.cs.scads.util.Logger._

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

class Tx(val timeout: Int, val readType: ReadConsistency = ReadConsistent())(mainFn: => Unit) {
  var unknownFn = () => {}
  var acceptFn = () => {}
  var commitFn = (status: TxStatus) => {}

  var updateList = new UpdateList
  var readList = new ReadList
  var protocolMap = new ProtocolMap

  protected val logger = Logger(classOf[Tx])

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

  def Execute(): TxStatus = {
    val startMS = java.util.Calendar.getInstance().getTimeInMillis()
    logger.info("BEGIN %s", Thread.currentThread.getName)
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
        logger.info("END %s", Thread.currentThread.getName)
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
