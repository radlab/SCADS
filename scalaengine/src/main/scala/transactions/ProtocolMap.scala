package edu.berkeley.cs.scads.storage.transactions

import edu.berkeley.cs.scads.comm._

import scala.collection.mutable.HashMap

import net.lag.logging.Logger

class ProtocolMap {
  private val protocolMap = new HashMap[String, TxProtocol]

  def addNamespaceProtocol(name: String,
                           protocol: TxProtocol) = {
    protocolMap.put(name, protocol)
  }

  // Returns the protocol to use for the transaction.  All namespaces must
  // use the same protocol.  If there are no writes, or there are conflicting
  // protocols, null is returned.
  def getProtocol() = {
    val protocolSet = protocolMap.values.toSet
    if (protocolSet.size == 1) {
      protocolSet.head
    } else {
      null
    }
  }

}
