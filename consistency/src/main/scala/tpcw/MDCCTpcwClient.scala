package edu.berkeley.cs
package scads
package consistency
package tpcw

import net.lag.logging.Logger

import edu.berkeley.cs.scads.piql._
import edu.berkeley.cs.scads.piql.plans._
import comm._
import storage._
import storage.client.index._

import edu.berkeley.cs.scads.piql.tpcw._
import edu.berkeley.cs.scads.storage.transactions._

import ch.ethz.systems.tpcw.populate.data.Utils

import java.util.UUID

// MDCC version of the tpcw client.
class MDCCTpcwClient(override val cluster: ScadsCluster, override val executor: QueryExecutor, val txProtocol: NSTxProtocol) extends TpcwClient(cluster, executor) {

  // transaction versions of the relations.
  override lazy val addresses = cluster.getNamespace[Address]("addresses", txProtocol)
  override lazy val authors = cluster.getNamespace[Author]("authors", txProtocol)
  override lazy val xacts = cluster.getNamespace[CcXact]("xacts", txProtocol)
  override lazy val countries = cluster.getNamespace[Country]("countries", txProtocol)
  override lazy val customers = cluster.getNamespace[Customer]("customers", txProtocol)
  override lazy val items = cluster.getNamespace[Item]("items", txProtocol)
  override lazy val orderLines = cluster.getNamespace[OrderLine]("orderLines", txProtocol)
  override lazy val orders = cluster.getNamespace[Order]("orders", txProtocol)
  override lazy val shoppingCartItems = cluster.getNamespace[ShoppingCartItem]("shoppingCartItems", txProtocol)

//  override val namespaces = List(addresses, authors, xacts, countries, customers, items, orderLines, orders, shoppingCartItems)
}
