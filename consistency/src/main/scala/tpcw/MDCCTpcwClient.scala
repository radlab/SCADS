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
  override val addresses = new PairNamespace[Address]("addresses", cluster, cluster.namespaces) with PairTransactions[Address] { override val protocolType = txProtocol }
  override val authors = new PairNamespace[Author]("authors", cluster, cluster.namespaces) with PairTransactions[Author] { override val protocolType = txProtocol }
  override val xacts = new PairNamespace[CcXact]("xacts", cluster, cluster.namespaces) with PairTransactions[CcXact] { override val protocolType = txProtocol }
  override val countries = new PairNamespace[Country]("countries", cluster, cluster.namespaces) with PairTransactions[Country] { override val protocolType = txProtocol }
  override val customers = new PairNamespace[Customer]("customers", cluster, cluster.namespaces) with PairTransactions[Customer] { override val protocolType = txProtocol }
  override val items = new PairNamespace[Item]("items", cluster, cluster.namespaces) with PairTransactions[Item] { override val protocolType = txProtocol }
  override val orderLines = new PairNamespace[OrderLine]("orderLines", cluster, cluster.namespaces) with PairTransactions[OrderLine] { override val protocolType = txProtocol }
  override val orders = new PairNamespace[Order]("orders", cluster, cluster.namespaces) with PairTransactions[Order] { override val protocolType = txProtocol }
  override val shoppingCartItems = new PairNamespace[ShoppingCartItem]("shoppingCartItems", cluster, cluster.namespaces) with PairTransactions[ShoppingCartItem] { override val protocolType = txProtocol }

  override val namespaces = List(addresses, authors, xacts, countries, customers, items, orderLines, orders, shoppingCartItems)
  namespaces.foreach(_.open)
}
