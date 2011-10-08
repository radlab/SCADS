package edu.berkeley.cs.scads.storage.transactions

import edu.berkeley.cs.scads.comm._



trait ProtocolBase {
  def RunProtocol(tx: Tx)

}
