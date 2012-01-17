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

  // Write transactions.
  override def shoppingCartWI(c_uname: String, newItems: Seq[(String, Int)]) = {
    val txStatus = new Tx(4000, ReadLocal()) ({
//      Thread.sleep(1000)
      val cart = retrieveShoppingCart(c_uname).map(_.head.asInstanceOf[ShoppingCartItem])
      val currentItems = cart.map(c => (c.SCL_I_ID, c.SCL_QTY))
      val itemsHash = new scala.collection.mutable.HashMap[String, Int]
      itemsHash ++= currentItems

      newItems.foreach(i => {
        val scl = new ShoppingCartItem(c_uname, i._1)
        if (itemsHash.contains(i._1)) {
          // Already exists in cart.
          val newQty = i._2 + itemsHash.getOrElse(i._1, 0)
          if (newQty == 0) {
            println("shoppingcartitem delete")
            shoppingCartItems.delete(scl)
          } else {
            println("shoppingcartitem logical")
            scl.SCL_QTY = i._2
            shoppingCartItems.asInstanceOf[PairTransactions[ShoppingCartItem]].putLogical(scl)
          }
        } else {
          // New item.
          println("shoppingcartitem put")
          scl.SCL_QTY = i._2
          shoppingCartItems.put(scl)
        }
      })
    }).Execute() match {
      case COMMITTED => true
      case _ => false
    }
    txStatus
  }

  override def buyConfirmWI(c_uname: String,
                            cc_type: String,
                            cc_number: Int,
                            cc_name: String,
                            cc_expiry: Long,
                            shipping: String): (String, Boolean) = {
    var result = ("", false)
    val txStatus = new Tx(4000, ReadLocal()) ({
      result = super.buyConfirmWI(c_uname, cc_type, cc_number, cc_name,
                                  cc_expiry, shipping)
    }).Execute() match {
      case COMMITTED => true
      case _ => false
    }
    (result._1, txStatus)
  }

  override def insertCustomer(cust: Customer) = {
    val txStatus = new Tx(4000, ReadLocal()) ({
      super.insertCustomer(cust)
    }).Execute() match {
      case COMMITTED => true
      case _ => false
    }
    txStatus
  }

  override def stockUpdates(cart: Seq[(ShoppingCartItem, Item)]): Seq[ScadsFuture[Unit]] = {
    if (txProtocol == NSTxProtocolMDCC()) {
      cart.map {
        case (scl, itm) => {
          val logicalItem = Item(itm.I_ID)
          logicalItem.I_TITLE = ""
          logicalItem.I_A_ID = ""
          logicalItem.I_PUB_DATE = 0
          logicalItem.I_PUBLISHER = ""
          logicalItem.I_SUBJECT = ""
          logicalItem.I_DESC = ""
          logicalItem.I_RELATED1 = 0
          logicalItem.I_RELATED2 = 0
          logicalItem.I_RELATED3 = 0
          logicalItem.I_RELATED4 = 0
          logicalItem.I_RELATED5 = 0
          logicalItem.I_THUMBNAIL = ""
          logicalItem.I_IMAGE = ""
          logicalItem.I_SRP = 0
          logicalItem.I_COST = 0
          logicalItem.I_AVAIL = 0
          logicalItem.I_STOCK = 0
          logicalItem.ISBN = ""
          logicalItem.I_PAGE = 0
          logicalItem.I_BACKING = ""
          logicalItem.I_DIMENSION = ""

          if (itm.I_STOCK - scl.SCL_QTY >= 10) {
            itm.I_STOCK = -scl.SCL_QTY
          } else {
            // ... uhh, what happens if this goes negative??? that's why i
            // put the min condition there (it's not given in the spec)
            itm.I_STOCK = -scl.SCL_QTY + 21
          }

          items.asInstanceOf[PairTransactions[Item]].putLogical(itm)
        }
      }
      List()
    } else {
      super.stockUpdates(cart)
    }
  }

  // Read only transactions.
  override def homeWI(args: Any*) = {
    var result: QueryResult = List()
    new Tx(4000, ReadLocal()) ({
      result = homeWIQuery(args:_*)
    }).Execute()
    result
  }

  override def newProductWI(args: Any*) = {
    var result: QueryResult = List()
    var elapsed:Long = 0
    new Tx(4000, ReadLocal()) ({
      val startTime = System.nanoTime / 1000000
      result = newProductWIQuery(args:_*)
      elapsed = System.nanoTime / 1000000 - startTime
    }).Execute()
//    println("newProductWI: " + elapsed)
    result
  }

  override def productDetailWI(args: Any*) = {
    var result: QueryResult = List()
    new Tx(4000, ReadLocal()) ({
      result = productDetailWIQuery(args:_*)
    }).Execute()
    result
  }

  override def searchByAuthorWI(args: Any*) = {
    var result: QueryResult = List()
    new Tx(4000, ReadLocal()) ({
      result = searchByAuthorWIQuery(args:_*)
    }).Execute()
    result
  }

  override def searchByTitleWI(args: Any*) = {
    var result: QueryResult = List()
    new Tx(4000, ReadLocal()) ({
      result = searchByTitleWIQuery(args:_*)
    }).Execute()
    result
  }

  override def searchBySubjectWI(args: Any*) = {
    var result: QueryResult = List()
    new Tx(4000, ReadLocal()) ({
      result = searchBySubjectWIQuery(args:_*)
    }).Execute()
    result
  }

  override def orderDisplayWI(c_uname: String, c_passwd: String, numOrderLinesPerPage: Int) = {
    var result: (Customer, Option[Order], Option[QueryResult]) = (Customer(""), None, None)
    new Tx(4000, ReadLocal()) ({
      result = super.orderDisplayWI(c_uname, c_passwd, numOrderLinesPerPage)
    }).Execute()
    (result._1, result._2, result._3)
  }

  override def buyRequestExistingCustomerWI(args: Any*) = {
    var result: QueryResult = List()
    new Tx(4000, ReadLocal()) ({
      result = buyRequestExistingCustomerWIQuery(args:_*)
    }).Execute()
    result
  }

  override def adminRequestWI(args: Any*) = {
    var result: QueryResult = List()
    new Tx(4000, ReadLocal()) ({
      result = adminRequestWIQuery(args:_*)
    }).Execute()
    result
  }

}
