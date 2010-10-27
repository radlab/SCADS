package edu.berkeley.cs.scads.piql

import edu.berkeley.cs.scads.storage._
import edu.berkeley.cs.avro.marker._

import org.apache.avro.util._
import scala.util.Random

class TpcwClient(val cluster: ScadsCluster, val executor: QueryExecutor) {
  lazy val address = cluster.getNamespace[AddressKey, AddressValue]("address")
  lazy val author = cluster.getNamespace[AuthorKey, AuthorValue]("author")
  lazy val authorNameItemIndex = cluster.getNamespace[AuthorNameItemIndexKey, NullRecord]("author_name_index")
  lazy val xacts = cluster.getNamespace[CcXactsKey, CcXactsValue]("xacts")
  lazy val country = cluster.getNamespace[CountryKey, CountryValue]("country")
  lazy val customer = cluster.getNamespace[CustomerKey, CustomerValue]("customer")
  lazy val item = cluster.getNamespace[ItemKey, ItemValue]("item")
  lazy val itemSubjectDateTitleIndex = cluster.getNamespace[ItemSubjectDateTitleIndexKey, ItemKey]("item_subject_date_title_index")
  lazy val orderline = cluster.getNamespace[OrderLineKey, OrderLineValue]("orderline")
  lazy val order = cluster.getNamespace[OrdersKey, OrdersValue]("orders")
  lazy val customerOrderIndex = cluster.getNamespace[CustomerOrderIndex, NullRecord]("customer_index")  //Extra index
  lazy val itemTitleIndex = cluster.getNamespace[ItemTitleIndexKey, NullRecord]("item_title_index")
  lazy val shoppingCartItem = cluster.getNamespace[ShoppingCartItemKey, ShoppingCartItemValue]("shopping_cart_item")

  // cardinality constraints
  // TODO: we need to place these in various queries
  val maxOrderLinesPerPage = 10
  val maxItemsPerCart = 5000

  def paraSelector(i : Int) = Array(ParameterValue(i))
  def projection(record: Int, attribute: Int) = Array(AttributeValue(record, attribute))
  val firstPara = paraSelector(0)

  implicit def toGeneric(ns: SpecificNamespace[_, _]) = ns.genericNamespace

  /**
   * Select DISTINCT C_FNAME,C_LNAME
   * from CUSTOMER
   * where C_UNAME=@C_UNAME
   */
  private lazy val homeWIPlan = IndexLookup(customer, Array(ParameterValue(0)))
  def homeWI(username: String): QueryResult =
    exec(homeWIPlan, new Utf8(username))

  /**
   * select top 50 I_ID,I_TITLE,A_FNAME,A_LNAME from ITEM , AUTHOR
   * where A_ID = I_A_ID AND I_SUBJECT LIKE @CategoryID
   * order by I_PUB_DATE desc,I_TITLE
   */
  //TODO Still missing the like operator. Does the like really work here???
  private lazy val newProductPlan = 
    IndexLookupJoin(  //(itemSubjectDateTitleIndex, itemKey, itemKey, ItemValue, authorKey, authorValue)
      author,
      projection(3, 1), //(itemSubjectDateTitleIndex, itemKey, itemKey, ItemValue)
      IndexLookupJoin(
        item,
        projection(1, 0),
        IndexScan(itemSubjectDateTitleIndex, firstPara, FixedLimit(50), true))
      )
  def newProductWI(subject: String): QueryResult =
    exec(newProductPlan, new Utf8(subject))

  /**
   * SELECT DISTINCT * FROM ITEM,AUTHOR
   * WHERE AUTHOR.A_ID = ITEM.I_A_ID AND ITEM.I_ID = @BookID
   */
  private lazy val productDetailPlan = 
    IndexLookupJoin( //(ItemKey, ItemValue, AuthorKey, AuthorValue)
      author,
      projection(1, 1), //(ItemKey, ItemValue)
      IndexLookup(item, Array(ParameterValue(0)))
    )
  def productDetailWI(bookId: String): QueryResult =
    exec(newProductPlan, bookId)

  /**
   * SELECT top 50 I_TITLE,I_ID,A_FNAME, A_LNAME
   * FROM ITEM,AUTHOR
   * WHERE I_A_ID = A_ID
   * AND ( A_LNAME LIKE '% ' + @Author + '%' OR A_LNAME LIKE @Author + '%' )
   * order by I_TITLE
   */
  //TODO: Alternatively, no limit just pagination!!!
  private lazy val searchByAuthorPlan =
    IndexLookupJoin( //(Name, I_TITLE, I_ID), null, ItemKey, ItemValue
      item,
      projection(0,2), //(Name, I_TITLE, I_ID), null
      IndexScan(authorNameItemIndex, firstPara, FixedLimit(50), true)
    )
  def searchByAuthorWI(name : String) = exec(searchByAuthorPlan, new Utf8(name))

  /**
   * SELECT TOP 50 I_TITLE, I_ID, A_FNAME, A_LNAME FROM ITEM, AUTHOR
   * WHERE I_A_ID = A_ID
   * AND ( I_TITLE LIKE '% ' + @Title + '%' OR I_TITLE LIKE @Title + '%' )
   * order by I_TITLE
   */
  private lazy val searchByTitlePlan =
    IndexLookupJoin( // (Token, Title, ID), NullRecord, (I_ID), ItemValue, AuthorKey, AuthorValue
      author,
      projection(3,1),
      IndexLookupJoin( // (Token, Title, ID), NullRecord, (I_ID), ItemValue
      item,
      projection(0,2), // (Token, Title, ID), NullRecord
      IndexScan(
        itemTitleIndex, 
        firstPara,
        FixedLimit(50),
        true
        )
      )
    )
  def searchByTitleWI(titleToken : String) = exec(searchByTitlePlan, new Utf8(titleToken.toLowerCase))

  /**
   * SELECT TOP 50 I_TITLE,I_ID, A_FNAME, A_LNAME
   * FROM ITEM, AUTHOR
   * WHERE I_A_ID = A_ID AND I_SUBJECT LIKE @CategoryID
   * order by I_TITLE
   */
  private lazy val searchBySubjectPlan =
   IndexLookupJoin( // (I_SUBJECT, I_PUB_DATE, I_TITLE),(I_ID), (I_ID), ItemValue, AuthorKey, AuthorValue
     author,
     projection(3,1), // (I_SUBJECT, I_PUB_DATE, I_TITLE),(I_ID), (I_ID), ItemValue
     IndexLookupJoin( // (I_SUBJECT, I_PUB_DATE, I_TITLE),(I_ID), (I_ID), ItemValue
       item,
       projection(1,0), // (I_SUBJECT, I_PUB_DATE, I_TITLE),(I_ID)
       IndexScan(itemSubjectDateTitleIndex, firstPara, FixedLimit(50), true)
     )
   )
  //TODO: this is the exact same query as newProductWI...
  def searchBySubjectWI(subject : String) = exec(searchBySubjectPlan, new Utf8(subject))

  /**
   * select C_ID from CUSTOMER where C_UNAME=@C_UNAME and C_PASSWD=@C_PASSWD
   *
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
   */


  private lazy val orderDisplayCustomerPlan =
    Selection(
      Equality(AttributeValue(1, 0), ParameterValue(1)),
      IndexLookup(customer, paraSelector(0))  // CustomerName, (C_PASSWD, C_FNAME, C_LNAME,....)
    )
  def orderDisplayCustomerWI(cName : String, cPassword : String) = 
    exec(orderDisplayCustomerPlan, new Utf8(cName), new Utf8(cPassword))

  private lazy val orderDisplayLastOrder =
    IndexLookupJoin( // (C_UNAME, O_DATE, O_ID), NullRecord, (O_ID), (O_C_ID, O_DATE_Time, ...) 
      order,
      Array(AttributeValue(0, 2)),
      StopAfter( // (C_UNAME, O_DATE, O_ID), NullRecord
        FixedLimit(1),
        IndexScan(
          customerOrderIndex, 
          firstPara,
          FixedLimit(1),
          false
        )
      )
    )
  def orderDisplayLastOrderWI(c_uname: String) =
    exec(orderDisplayLastOrder, new Utf8(c_uname))

  private lazy val orderDisplayGetAddressInfo =
    IndexLookupJoin( // (ADDR_ID), (ADDR_STREET_1, ...), (CO_ID), (CO_NAME, ...)
      country,
      Array(AttributeValue(1, 5)),
      IndexLookup(address, firstPara) // (ADDR_ID), (ADDR_STREET_1, ...)
    )
  def orderDisplayGetAddressInfoWI(addr_id: String) = 
    exec(orderDisplayGetAddressInfo, new Utf8(addr_id))

  private lazy val orderDisplayGetOrderLines =
    IndexLookupJoin( // (OL_O_ID, OL_ID), (OL_I_ID, ...), (I_ID), (I_TITLE, ...) 
      item,
      Array(AttributeValue(1, 0)), 
      StopAfter( // (OL_O_ID, OL_ID), (OL_I_ID, ...)
        ParameterLimit(1, maxOrderLinesPerPage),
        IndexScan(
          orderline,
          firstPara,
          ParameterLimit(1, maxOrderLinesPerPage),
          true
        )
      )
    )
  def orderDisplayGetOrderLinesWI(o_id: String, numOrderLinesPerPage: Int) = 
    exec(orderDisplayGetOrderLines, new Utf8(o_id), numOrderLinesPerPage)


  def orderDisplayWI(c_uname: String, c_passwd: String, numOrderLinesPerPage: Int) = {
    val cust = orderDisplayCustomerWI(c_uname, c_passwd) 
    assert(!cust.isEmpty, "No user found with UNAME %s, PASSWD %s".format(c_uname, c_passwd))
    val lastOrder = orderDisplayLastOrderWI(c_uname)
    if (lastOrder.isEmpty) Seq.empty // no orders for this user
    else {
      val billingAddrAndCo = orderDisplayGetAddressInfoWI(lastOrder(0)(3).get(7).toString)
      val shippingAddrAndCo = orderDisplayGetAddressInfoWI(lastOrder(0)(3).get(8).toString)
      val orderLines = orderDisplayGetOrderLinesWI(lastOrder(0)(2).get(0).toString, numOrderLinesPerPage)
      Seq(cust(0) ++ lastOrder(0) ++ billingAddrAndCo(0) ++ shippingAddrAndCo(0) ++ orderLines.flatten)
    }
  }

  def newShoppingCart(c_uname: String) : (String, Long) = 
    (c_uname, System.currentTimeMillis)


  /**
   * select C_ID from CUSTOMER where C_UNAME=@C_UNAME and
   * C_PASSWD=@C_PASSWD

   * SELECT C_UNAME,C_PASSWD,C_FNAME,C_LNAME,C_PHONE,
   * C_EMAIL,C_BIRTHDATE,C_DATA1,C_DATA2,ADDR_STREET1,
   * ADDR_STREET2,ADDR_CITY,ADDR_STATE,ADDR_ZIP,CO_NAME
   * FROM CUSTOMER,ADDRESS,COUNTRY
   * where C_ADDR_ID=ADDR_ID and ADDR_CO_ID=CO_ID and C_ID = @C_ID
   */

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
  }

  def exec(plan: QueryPlan, args: Any*) = {
    val iterator = executor(plan, args:_*)
    iterator.open
    iterator.toSeq
  }

  //def loadData(numEBs : Double, numItems : Int) = {
  //  val loader = new TpcwLoader(this, numEBs, numItems)
  //  loader.load()
  //}

}
