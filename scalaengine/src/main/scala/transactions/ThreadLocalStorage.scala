package edu.berkeley.cs.scads.storage.transactions

import scala.util.DynamicVariable

import net.lag.logging.Logger

object ThreadLocalStorage {
  // Records updated within the tx.
  val updateList = new DynamicVariable[Option[UpdateList]](None)

  // Records read from within the tx.
  val txReadList = new DynamicVariable[Option[ReadList]](None)

  // Holds all protocols for all namespaces in the tx.
  val protocolMap = new DynamicVariable[Option[ProtocolMap]](None)

  val readConsistency = new DynamicVariable[ReadConsistency](ReadConsistent())
}
