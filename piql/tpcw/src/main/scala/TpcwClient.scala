package edu.berkeley.cs
package scads
package piql
package tpcw

import net.lag.logging.Logger

import scads.storage._
import avro.marker._
import avro.runtime._

import ch.ethz.systems.tpcw.populate.data.Utils

import org.apache.avro.util._
import scala.util.Random
import scala.collection.mutable.{ Map => MMap }

import java.util.UUID

class TpcwClient(val cluster: ScadsCluster, val executor: QueryExecutor) {
  protected val logger = Logger("edu.berkeley.cs.scads.piql.TpcwWorkflow")
  protected implicit val exec = executor

  /* Relations */
  lazy val addresses = cluster.getNamespace[Address]("addresses")
  lazy val authors = cluster.getNamespace[Author]("authors")
  lazy val xacts = cluster.getNamespace[CcXact]("xacts")
  lazy val countries = cluster.getNamespace[Country]("countries")
  lazy val customers = cluster.getNamespace[Customer]("customers")
  lazy val items = cluster.getNamespace[Item]("items")
  lazy val orderLines = cluster.getNamespace[OrderLine]("orderLines")
  lazy val orders = cluster.getNamespace[Order]("orders")
  lazy val shoppingCartItems = cluster.getNamespace[ShoppingCartItem]("shoppingCartItems")

  // cardinality constraints
  // TODO: we need to place these in various queries
  val maxOrderLinesPerPage = 100
  val maxItemsPerCart = 5000

  /**
   * Home web interaction
   * 
   * Select DISTINCT C_FNAME,C_LNAME
   * from CUSTOMER
   * where C_UNAME=@C_UNAME
   */
  val homeWI = customers.where("C_UNAME".a === (0.?))
		        .toPiql("homeWI")

  /**
   * New Products web interaction
   * 
   * select top 50 I_ID,I_TITLE,A_FNAME,A_LNAME from ITEM , AUTHOR
   * where A_ID = I_A_ID AND I_SUBJECT LIKE @CategoryID
   * order by I_PUB_DATE desc,I_TITLE
  //TODO Still missing the like operator. Does the like really work here???
  private lazy val newProductPlan =
    IndexLookupJoin(  //(itemSubjectDateTitleIndex, itemKey, itemKey, ItemValue, authorKey, authorValue)
      author,
      projection(3, 1), //(itemSubjectDateTitleIndex, itemKey, itemKey, ItemValue)
      IndexLookupJoin(
        item,
        projection(1, 0),
        StopAfter(
          FixedLimit(50),
          IndexScan(itemSubjectDateTitleIndex, firstPara, FixedLimit(50), true))
      )
    )

  def newProductWI(subject: String): QueryResult =
    exec(newProductPlan, new Utf8(subject))
    */

  /**
   * Best Sellers web interactio
   *
   *  declare @last_o numeric(10)
   * select top 3333  O_ID into #temp  from ORDERS
   * order by O_DATE desc
   * select @last_o = min(O_ID) from #temp
   * select top 50 I_ID,I_TITLE,A_FNAME,A_LNAME
   * from ITEM, AUTHOR ,ORDER_LINE
   * where OL_O_ID > @last_o AND
   * I_ID = OL_I_ID AND I_A_ID = A_ID AND I_SUBJECT = @CategoryID
   * group by I_ID,I_TITLE,A_FNAME,A_LNAME
   * order by SUM(OL_QTY)  desc
   */

  //NOT IMPLEMENTED



  /**
   * Product Detail web interaction
   * 
   * SELECT DISTINCT * FROM ITEM,AUTHOR
   * WHERE AUTHOR.A_ID = ITEM.I_A_ID AND ITEM.I_ID = @BookID
   */
  val productDetailWI = items.where("I_ID".a === (0.?))
			     .join(authors)
			     .where("A_ID".a === "I_A_ID")
			     .toPiql("productDetailWI")
  

  /**
   * Search Result web interaction (by Author)
   * 
   * SELECT top 50 I_TITLE,I_ID,A_FNAME, A_LNAME
   * FROM ITEM,AUTHOR
   * WHERE I_A_ID = A_ID
   * AND ( A_LNAME LIKE '% ' + @Author + '%' OR A_LNAME LIKE @Author + '%' )
   * order by I_TITLE
  //TODO: Alternatively, no limit just pagination!!!
  private lazy val searchByAuthorPlan =
    IndexLookupJoin( //(Name, I_TITLE, I_ID), null, ItemKey, ItemValue
      item,
      projection(0,2), //(Name, I_TITLE, I_ID), null
      StopAfter(
        FixedLimit(50),
        IndexScan(authorNameItemIndex, firstPara, FixedLimit(50), true)
      )
    )
  def searchByAuthorWI(name : String) = exec(searchByAuthorPlan, new Utf8(name))
  */

  /**
   * Search Result web interaction (by Title)
   * 
   * SELECT TOP 50 I_TITLE, I_ID, A_FNAME, A_LNAME FROM ITEM, AUTHOR
   * WHERE I_A_ID = A_ID
   * AND ( I_TITLE LIKE '% ' + @Title + '%' OR I_TITLE LIKE @Title + '%' )
   * order by I_TITLE
  private lazy val searchByTitlePlan =
    IndexLookupJoin( // (Token, Title, ID), NullRecord, (I_ID), ItemValue, AuthorKey, AuthorValue
      author,
      projection(3,1),
      IndexLookupJoin( // (Token, Title, ID), NullRecord, (I_ID), ItemValue
        item,
        projection(0,2), // (Token, Title, ID), NullRecord
        StopAfter(
          FixedLimit(50),
          IndexScan(
            itemTitleIndex,
            firstPara,
            FixedLimit(50),
            true
          )
        )
      )
    )
  def searchByTitleWI(titleToken : String) = exec(searchByTitlePlan, new Utf8(titleToken.toLowerCase))
  */
 

  /**
   * Search Result web interaction (by Subject)
   * 
   * SELECT TOP 50 I_TITLE,I_ID, A_FNAME, A_LNAME
   * FROM ITEM, AUTHOR
   * WHERE I_A_ID = A_ID AND I_SUBJECT LIKE @CategoryID
   * order by I_TITLE
    private lazy val searchBySubjectPlan =
    IndexLookupJoin( // (I_SUBJECT, I_PUB_DATE, I_TITLE),(I_ID), (I_ID), ItemValue, AuthorKey, AuthorValue
      author,
      projection(3,1), // (I_SUBJECT, I_PUB_DATE, I_TITLE),(I_ID), (I_ID), ItemValue
      IndexLookupJoin( // (I_SUBJECT, I_PUB_DATE, I_TITLE),(I_ID), (I_ID), ItemValue
        item,
        projection(1,0), // (I_SUBJECT, I_PUB_DATE, I_TITLE),(I_ID)
        StopAfter(
          FixedLimit(50),
          IndexScan(itemSubjectDateTitleIndex, firstPara, FixedLimit(50), true)
        )
      )
    )
  //TODO: this is the exact same query as newProductWI...
  def searchBySubjectWI(subject : String) = exec(searchBySubjectPlan, new Utf8(subject))
  */

  /**
   * Order Display web interaction
   *
   * FirstQuery:
   * select C_ID from CUSTOMER where C_UNAME=@C_UNAME and C_PASSWD=@C_PASSWD
   * DECLARE @O_ID numeric(10) select @O_ID = max(O_ID)from ORDERS where
   * O_C_ID=@C_ID
   * SELECT
   *   C_FNAME,C_LNAME,C_EMAIL,C_PHONE,
   *   O_ID,O_DATE,O_SUBTOTAL,O_TAX,O_TOTAL,O_SHIP_TYPE,O_SHIP_DATE,
   *   O_BILL_ADDR,O_SHIP_ADDR,O_CC_TYPE,O_STATUS,
   *   ADDR_STREET1,ADDR_STREET2,ADDR_CITY,ADDR_STATE,ADDR_ZIP,CO_NAME
   * FROM CUSTOMER,ADDRESS,COUNTRY,ORDERS
   * where
   *   O_ID=@O_ID and
   *   C_ID=@C_ID and
   *   O_BILL_ADDR=ADDR_ID AND
   *   ADDR_CO_ID=CO_ID
   * Select ADDR_STREET1,ADDR_STREET2,ADDR_CITY,
   * ADDR_STATE,ADDR_ZIP,CO_NAME from ADDRESS,COUNTRYwhere
   * ADDR_ID=@A_ID and ADDR_CO_ID=CO_ID
   *
   * SecondQuery:
   * select OL_I_ID,I_TITLE,I_PUBLISHER,
   * I_COST,OL_QTY,OL_DISCOUNT,OL_COMMENTSfrom ORDER_LINE,ITEM where
   * OL_I_ID=I_ID and OL_O_ID=@O_I
   */
  val orderDisplayGetCustomer =
    customers.where("C_UNAME".a === (0.?))
	     .toPiql("orderDisplayGetCustomer")

  val orderDisplayGetLastOrder =
    orders
      .where("O_C_UNAME".a === (0.?))
      .sort("O_DATE_Time".a :: Nil, false)
      .limit(1)
      .join(addresses)
      .where("ADDR_ID".a === "O_BILL_ADDR_ID".a)
      .join(countries)
      .where("ADDR_CO_ID".a === "CO_ID".a)
      .join(addresses)
      .where("ADDR_ID".a === "O_SHIP_ADDR_ID".a)
      .join(countries)
      .where("ADDR_CO_ID".a === "CO_ID".a)
      .toPiql("orderDisplayGetLastOrder")

  val orderDisplayGetOrderLines =
    orderLines.where("OL_O_ID".a === (0.?))
      .limit((1.?), maxOrderLinesPerPage)
      .join(items)
      .where("OL_I_ID".a === "I_ID".a)
      .toPiql("orderDisplayGetOrderLines")


  def orderDisplayWI(c_uname: String, c_passwd: String, numOrderLinesPerPage: Int) = {
    val cust = orderDisplayGetCustomer(c_uname).head.head.asInstanceOf[Customer]
    assert(cust.C_PASSWD == c_passwd, "Passwords don't match")

    val lastOrderDetails = orderDisplayGetLastOrder(c_uname).headOption
    val lastOrder = lastOrderDetails.map(_.apply(0).asInstanceOf[Order])
    val lines = lastOrder.map(o => orderDisplayGetOrderLines(o.O_ID, numOrderLinesPerPage))

    (cust, lastOrder, lines)
  }

  /**
   * 
  private lazy val retrieveShoppingCartPlan =
    StopAfter(
      FixedLimit(100),
      IndexScan( // (C_UNAME, ...), (SCL_QTY, ...)
        shoppingCartItem,
        Array(ParameterValue(0)),
        FixedLimit(100), // opt limit imposed
        true
      )
    )
  def retrieveShoppingCart(c_uname: String) =
    exec(retrieveShoppingCartPlan, new Utf8(c_uname))

  private lazy val retrieveItemPlan =
    IndexLookup(
      item,
      firstPara)
  def retrieveItem(itemId: String) =
    exec(retrieveItemPlan, new Utf8(itemId))
    *
  */

  /**
   * This is a very simplified shopping cart WI.
   * Given a c_uname, items will be added to the cart, or the
   * quantity will be updated. this is not really conformant to the TPC-W
   * benchmark spec
  def shoppingCartWI(c_uname: String, items: Seq[(String, Int)]) = {
    val cart = retrieveShoppingCart(c_uname) map { case Array(k, v) =>
      (k.toSpecificRecord[ShoppingCartItemKey], v.toSpecificRecord[ShoppingCartItemValue])
    }
    val itemsMap = MMap(items:_*)
    cart.foreach { case (k, v) =>
      itemsMap.remove(k.SCL_I_ID) match {
        case Some(qty) => v.SCL_QTY = qty
        case None => // no action
      }
    }
    val newCart = cart ++ itemsMap.map { case (k, v) =>
      val item      = retrieveItem(k)
      val itemKey   = item(0)(0).toSpecificRecord[ItemKey]
      val itemValue = item(0)(1).toSpecificRecord[ItemValue]

      val title   = itemValue.I_TITLE
      val cost    = itemValue.I_COST
      val srp     = itemValue.I_SRP
      val backing = itemValue.I_BACKING

      (ShoppingCartItemKey(c_uname, k), ShoppingCartItemValue(v, cost, srp, title, backing))
    }
    shoppingCartItem ++= newCart
  }
  */

  /**
   * select C_ID from CUSTOMER where C_UNAME=@C_UNAME and
   * C_PASSWD=@C_PASSWD

   * SELECT C_UNAME,C_PASSWD,C_FNAME,C_LNAME,C_PHONE,
   * C_EMAIL,C_BIRTHDATE,C_DATA1,C_DATA2,ADDR_STREET1,
   * ADDR_STREET2,ADDR_CITY,ADDR_STATE,ADDR_ZIP,CO_NAME
   * FROM CUSTOMER,ADDRESS,COUNTRY
   * where C_ADDR_ID=ADDR_ID and ADDR_CO_ID=CO_ID and C_ID = @C_ID

  private def buyRequestCustomerWI(cName : String, cPassword : String) =
    exec(orderDisplayCustomerPlan, new Utf8(cName), new Utf8(cPassword))

  private lazy val buyRequestAddrCoPlan =
    IndexLookupJoin( // (C_UNAME), (C_PASSWD, ...), (ADDR_ID), (ADDR_STREET1, ...), (CO_ID), (CO_NAME, ...)
      country,
      Array(AttributeValue(3, 5)),
      IndexLookupJoin( // (C_UNAME), (C_PASSWD, ...), (ADDR_ID), (ADDR_STREET1, ...)
        address,
        Array(AttributeValue(1, 3)),
        IndexLookup( // (C_UNAME), (C_PASSWD, ...)
          customer,
          firstPara
        )
      )
    )
  private def buyRequestAddrCoWI(c_uname: String) =
    exec(buyRequestAddrCoPlan, new Utf8(c_uname))

  def buyRequestExistingWI(c_uname: String, c_passwd: String) = {
    val cust = orderDisplayCustomerWI(c_uname, c_passwd)
    assert(!cust.isEmpty, "No user found with UNAME %s, PASSWD %s".format(c_uname, c_passwd))
    buyRequestAddrCoWI(c_uname)
    val (k, v) = (cust(0)(0).toSpecificRecord[CustomerKey],
                  cust(0)(1).toSpecificRecord[CustomerValue])
    v.C_LOGIN = System.currentTimeMillis
    v.C_EXPIRATION = v.C_LOGIN + (2L * 3600000L) // +2 hrs in millis
    customer.put(k, v) // save
  }
  */

  /**
   * -- BEGIN WALL OF SQL --
   * DECLARE @CO_ID numeric(4)
   * DECLARE @ADDR_ID numeric(10)
   *
   * Select @CO_ID = CO_ID from COUNTRY where CO_NAME=@CO_NAME
   *
   * SELECT ADDR_ID
   * FROM ADDRESS
   * WHERE
   * ADDR_STREET1=@ADDR_STREET1 and
   * ADDR_STREET2=@ADDR_STREET2 and
   * ADDR_CITY=@ADDR_CITY and
   * ADDR_STATE=@ADDR_STATE and
   * ADDR_ZIP=@ADDR_ZIP and
   * ADDR_CO_ID=@CO_ID
   *
   * Select @CO_ID = CO_ID
   * from COUNTRY
   * where CO_NAME=@CO_NAME
   *
   * Insert into ADDRESS values(@ADDR_STREET1,
   * @ADDR_STREET2,@ADDR_CITY,@ADDR_STATE,@ADDR_ZIP,
   * @CO_ID)
   *
   * select @ADDR_ID = @@identity
   *
   * select C_ID,C_DISCOUNT,C_ADDR_ID
   * from CUSTOMER
   * where C_UNAME=@UserID
   *
   * DECLARE @O_ID numeric(9)
   *
   * Insert into ORDERS values (@O_C_ID,getdate(),@O_SUBTOTAL,
   * @O_TAX,@O_TOTAL,@O_SHIP_TYPE,NULL,@O_BILL_ADDR,
   * @O_SHIP_ADDR,@O_CC_TYPE,@O_CC_NUM,@O_CC_NAME,
   * @O_CC_EXP,'Pending')
   *
   * select @O_ID = @@identity
   *
   * Insert ORDER_LINE (OL_O_ID,OL_I_ID,OL_QTY,
   * OL_DISCOUNT,OL_COMMENTS)
   *
   * Select @O_ID,SC_I_ID,SC_QTY,1,'comment' from SHOPPING_CART
   *
   * update ITEM     set I_STOCK = I_STOCK - SCL_I_QTY + case when (I_STOCK -
   * SCL_I_QTY < 10) then 21 else 0 end
   * from SHOPPING_BASKET
   * where SC_SHOPPING_ID=@Session and SC_HOST=@SC_HOST and SCL_I_ID =
   * I_ID
   *
   * Delete from SHOPPING_CART where SC_ID=@Session
   *
   * Insert into CC_XACTS
   * values(@O_ID,@O_CC_TYPE,@O_CC_EXP,@O_CC_AUTH,@O_TOTAL,getdate(),@CO_ID)

  /**
   * Returns the order ID
   */
  def buyConfirmWI(c_uname: String,
                   cc_type: String,
                   cc_number: Int,
                   cc_name: String,
                   cc_expiry: Long,
                   shipping: String): String = {
    val (userKey, userValue) = homeWI(c_uname) map { case Array(k, v) =>
      (k.toSpecificRecord[CustomerKey], v.toSpecificRecord[CustomerValue])
    } head
    val cart = retrieveShoppingCart(c_uname) map { case Array(k, v) =>
      (k.toSpecificRecord[ShoppingCartItemKey], v.toSpecificRecord[ShoppingCartItemValue])
    }

    // calculate costs
    val sc_sub_total = (cart.foldLeft(0.0) { case (acc, (k, v)) =>
      acc + v.SCL_COST * v.SCL_QTY.toDouble
    }) * (1.0 - userValue.C_DISCOUNT)

    val sc_tax = sc_sub_total * 0.0825
    val sc_ship_cost = 3.0 + (1.0 * cart.size.toDouble)
    val sc_total = sc_sub_total + sc_tax + sc_ship_cost

    def newUUID =
      UUID.randomUUID.toString

    // make order
    val orderKey = OrdersKey(newUUID)
    val orderValue = OrdersValue(
      c_uname,
      System.currentTimeMillis,
      sc_sub_total,
      sc_tax,
      sc_total,
      shipping,
      System.currentTimeMillis + (scala.util.Random.nextInt(7) + 1).toLong * 86400L, // [1..7] days later
      userValue.C_ADDR_ID,
      userValue.C_ADDR_ID,
      "PENDING")

    // make order secondary indexes
    val customerOrderIndexKey = CustomerOrderIndex(c_uname, orderValue.O_DATE_Time, orderKey.O_ID)

    // make order lines
    val orderLines = cart.zipWithIndex.map { case ((k, v), idx) =>
      (OrderLineKey(orderKey.O_ID, idx + 1),
       OrderLineValue(
         k.SCL_I_ID,
         v.SCL_QTY,
         userValue.C_DISCOUNT,
         Utils.getRandomAString(20, 100)))
    }

    // make item stocks updates
    val items = cart map { case (k, v) =>
      val (itemKey, itemValue) = retrieveItem(k.SCL_I_ID) map { case Array(k0, v0) =>
        (k0.toSpecificRecord[ItemKey], v0.toSpecificRecord[ItemValue])
      } head

      // update conditions given in clause 2.7.3.3
      if (itemValue.I_STOCK - v.SCL_QTY >= 10)
        itemValue.I_STOCK -= v.SCL_QTY
      else
        itemValue.I_STOCK = scala.math.min(0, (itemValue.I_STOCK - v.SCL_QTY) + 21) // ... uhh, what happens if this goes negative??? that's why i put the min condition there (it's not given in the spec)

      (itemKey, itemValue)
    }

    // credit card (PGE) auth stuff ignored...

    // make cc txn
    val ccXactsKey = CcXactsKey(orderKey.O_ID)
    val ccXactsValue = CcXactsValue(
      cc_type,
      cc_number,
      cc_name,
      cc_expiry,
      Utils.getRandomAString(15),
      sc_total,
      System.currentTimeMillis,
      Utils.getRandomInt(1, 92))


    // do the actual updates. first do the writes. NOTE that in the TPC-W spec
    // this is (obviously) supposed to be atomic, but we're not gonna do that
    // (eventual consistency FTW)
    order.put(orderKey, Some(orderValue))
    customerOrderIndex.put(customerOrderIndexKey, Some(NullRecord(true)))
    orderline ++= orderLines
    item ++= items
    xacts.put(ccXactsKey, Some(ccXactsValue))

    // clear shopping cart. unfortunately BulkPut does not support deletion,
    // so we do this inefficiently by looping. if our numbers are not good we
    // could try to make this more efficient, but since max cart size is 100,
    // this shouldn't be THAT bad
    cart foreach { case (k, _) =>
      shoppingCartItem.put(k, None) // delete
    }

    logger.debug("finished buy confirmation of %d items", cart.size)

    orderKey.O_ID
  }
  */

  /** Identical query to productDetailWI
  def adminRequestWI(bookId: String) =
    productDetailWI(bookId)
    */
}
