package edu.berkeley.cs.scads.piql

import edu.berkeley.cs.scads.comm._
import edu.berkeley.cs.scads.storage._
import edu.berkeley.cs.avro.marker._

import edu.berkeley.cs.scads.piql.DataGenerator._
import ch.ethz.systems.tpcw.populate.data.Generator
import java.util.UUID
import ch.ethz.systems.tpcw.populate.data.objects._
import collection.mutable.{ArrayBuffer, HashMap}


class TpcwLoader( val client : TpcwClient,
                  val numClients: Int, // Number of LOADING clients
                  val numEBs : Double,
                  val numItems : Int ) {

  require(client != null)
  require(numClients >= 1)
  require(numItems >= 1)

	val numCustomers : Int = (numEBs * 2880).intValue;
	val numAddresses : Int = 2 * numCustomers;
	val numAuthors : Int = (.25 * numItems).intValue;
	val numOrders : Int = (.9 * numCustomers).intValue;
  val numCountries = 92;

  //private var addressIds = new  HashMap[Int, String]()
	//private var orderIds = new  HashMap[Int, String]()
	//private var itemIds  = new  HashMap[Int, String]()
	////private var orderDates = new  HashMap[Int, String]() Was used in SimpleDB
  //private var authors = new  HashMap[Int, (AuthorKey, AuthorValue)]()
  //private var AuthorNameItemIndexInserts = ArrayBuffer[(AuthorNameItemIndexKey, NullRecord)]()
  //private var itemSubjectDateTitleIndexInserts = ArrayBuffer[(ItemSubjectDateTitleIndexKey, ItemKey)]()
  //private var customerOrderIndexInserts = ArrayBuffer[(CustomerOrderIndex, NullRecord)]()
  //private var itemTitleIndexInserts = ArrayBuffer[(ItemTitleIndexKey, NullRecord)]()

  // TODO: currentTimeMillis is not as random of a seed as is provided by the
  // default no arg ctor (which also uses hashCode of the Random object)
  val rand = new scala.util.Random(System.currentTimeMillis)

  private def uuid() : String = UUID.randomUUID.toString

  private var ctr : Int = 10000

  private def id(c_name : String) : String = {
    ctr += 1
    if(ctr > 99999) ctr = 10000
    c_name + System.currentTimeMillis + ctr;
  }

  private def toCustomer(id: Int) = 
    "cust%d".format(id)

  private def toAuthor(id: Int) =
    "author%d".format(id)

  private def toAuthorFname(id: Int) =
    "author_fname%d".format(id)

  private def toAuthorLname(id: Int) =
    "author_lname%d".format(id)

  private def toAddress(id: Int) =
    "address%d".format(id)

  private def toXact(id: Int) = 
    "xact%d".format(id)

  private def toItem(id: Int) =
    "item%d".format(id)

  private def toOrder(id: Int) =
    "order%d".format(id)

  private def fromAuthor(author: String) =
    author.drop(6).toInt

  def keySplits: TpcwKeySplits = {
    val servers = client.cluster.getAvailableServers
    val clusterSize = servers.size
    TpcwKeySplits(
      List((None, servers)),
      List((None, servers)),
      List((None, servers)),
      List((None, servers)),
      List((None, servers)),
      List((None, servers)),
      List((None, servers)),
      List((None, servers)),
      List((None, servers)),
      List((None, servers)),
      List((None, servers)),
      List((None, servers)),
      List((None, servers))
      )
  }

  case class TpcwKeySplits(
    address: Seq[(Option[AddressKey], List[StorageService])],
    author: Seq[(Option[AuthorKey], List[StorageService])],
    authorname_item_index: Seq[(Option[AuthorNameItemIndexKey], List[StorageService])],
    xacts: Seq[(Option[CcXactsKey], List[StorageService])],
    country: Seq[(Option[CountryKey], List[StorageService])],
    customer: Seq[(Option[CustomerKey], List[StorageService])],
    item: Seq[(Option[ItemKey], List[StorageService])],
    item_subject_date_title_index: Seq[(Option[ItemSubjectDateTitleIndexKey], List[StorageService])],
    orderline: Seq[(Option[OrderLineKey], List[StorageService])],
    orders: Seq[(Option[OrdersKey], List[StorageService])],
    customer_index: Seq[(Option[CustomerOrderIndex], List[StorageService])] ,
    title_index : Seq[(Option[ItemTitleIndexKey], List[StorageService])],
    shopping_cart : Seq[(Option[ShoppingCartItemKey], List[StorageService])]
    )


  def createNamespaces() = {
    val splits = keySplits
    client.cluster.createNamespace[AddressKey, AddressValue]("address", splits.address)
    client.cluster.createNamespace[AuthorKey, AuthorValue]("author", splits.author)
    client.cluster.createNamespace[AuthorNameItemIndexKey, NullRecord]("author_fname_index", splits.authorname_item_index)
    client.cluster.createNamespace[CcXactsKey, CcXactsValue]("xacts", splits.xacts)
    client.cluster.createNamespace[CountryKey, CountryValue]("country", splits.country)
    client.cluster.createNamespace[CustomerKey, CustomerValue]("customer", splits.customer)
    client.cluster.createNamespace[ItemKey, ItemValue]("item", splits.item)
    client.cluster.createNamespace[ItemSubjectDateTitleIndexKey, ItemKey]("item_subject_date_title_index", splits.item_subject_date_title_index)
    client.cluster.createNamespace[OrderLineKey, OrderLineValue]("orderline", splits.orderline)
    client.cluster.createNamespace[OrdersKey, OrdersValue]("orders", splits.orders)
    client.cluster.createNamespace[CustomerOrderIndex, NullRecord]("customer_index", splits.customer_index)  //Extra index
    client.cluster.createNamespace[ItemTitleIndexKey, NullRecord]("item_title_index", splits.title_index)
    client.cluster.createNamespace[ShoppingCartItemKey, ShoppingCartItemValue]("shopping_cart_item", splits.shopping_cart)
  }

  case class TpcwData(
      // main objects
      addresses: Seq[(AddressKey, AddressValue)],
      authors: Seq[(AuthorKey, AuthorValue)],
      xacts: Seq[(CcXactsKey, CcXactsValue)],
      countries: Seq[(CountryKey, CountryValue)],
      customers: Seq[(CustomerKey, CustomerValue)],
      items: Seq[(ItemKey, ItemValue)],
      orders: Seq[(OrdersKey, OrdersValue)], 
      orderlines: Seq[(OrderLineKey, OrderLineValue)],

      // secondary/inverted indicies
      authorNameItemIndexes: Seq[(AuthorNameItemIndexKey, NullRecord)],
      itemSubjectDateTitleIndexes: Seq[(ItemSubjectDateTitleIndexKey, ItemKey)],
      customerOrderIndexes: Seq[(CustomerOrderIndex, NullRecord)],
      itemTitleIndexes: Seq[(ItemTitleIndexKey, NullRecord)]
    ) {

    def load() = {
      //createNamespaces()
      //client.address ++= (1 to numAddresses).view.map(createAddress(_))
      //client.author ++= (1 to numAuthors).view.map(createAuthor(_))
      //client.xacts ++= (1 to numOrders).view.map(createXacts(_))
      //client.country ++= (1 to numCountries).view.map(createCountry(_))
      //client.customer ++= (1 to numCustomers).view.map(createCustomer(_))
      //client.item ++= (1 to numItems).view.map(createItem(_))
      //client.authorNameItemIndex ++= AuthorNameItemIndexInserts
      //AuthorNameItemIndexInserts.clear
      //client.itemSubjectDateTitleIndex ++= itemSubjectDateTitleIndexInserts
      //itemSubjectDateTitleIndexInserts.clear
      //client.order ++= (1 to numOrders).view.map(createOrder(_))
      //client.orderline ++= (1 to numOrders).view.flatMap(createOrderline(_))
      //client.customerOrderIndex ++=  customerOrderIndexInserts
      //customerOrderIndexInserts.clear

      client.address ++= addresses
      client.author ++= authors
      client.xacts ++= xacts
      client.country ++= countries
      client.customer ++= customers
      client.item ++= items
      client.order ++= orders
      client.orderline ++= orderlines

      client.authorNameItemIndex ++= authorNameItemIndexes
      client.itemSubjectDateTitleIndex ++= itemSubjectDateTitleIndexes
      client.customerOrderIndex ++= customerOrderIndexes
      client.itemTitleIndex ++= itemTitleIndexes
    }
  }

  def getData(clientId: Int, useViews: Boolean = true) : TpcwData = {
    def newRange(upperBound: Int) = 
      if (useViews) (1 to upperBound).view else (1 to upperBound)

    val addresses = newRange(numAddresses).map(createAddress(_))
    val authors = newRange(numAuthors).map(createAuthor(_))
    val xacts = newRange(numOrders).map(createXacts(_))
    val countries = newRange(numCountries).map(createCountry(_))
    val customers = newRange(numCustomers).map(createCustomer(_))

    // these two can't be views b/c we need each invocation to be
    // deterministic
    //val items = (1 to numItems).view.map(createItem(_))
    //val orders = (1 to numOrders).view.map(createOrder(_))
    val items = (1 to numItems).map(createItem(_))
    val orders = (1 to numOrders).map(createOrder(_))

    val orderlines = newRange(numOrders).flatMap(createOrderline(_))

    val authorNameItemIndexes = items.flatMap(createAuthorNameItemIndexes(_))
    val itemSubjectDateTitleIndexes = items.map(createItemSubjectDateTitleIndex(_))
    val customerOrderIndexes = orders.map(createCustomerOrderIndex(_)) 
    val itemTitleIndexes = items.flatMap(createItemTitleIndex(_)) 
       
    TpcwData(
      addresses,
      authors,
      xacts,
      countries,
      customers,
      items,
      orders, 
      orderlines,

      authorNameItemIndexes,
      itemSubjectDateTitleIndexes,
      customerOrderIndexes,
      itemTitleIndexes)
  }

  def createItem(itemId : Int) : (ItemKey, ItemValue) = {
    val to = Generator.generateItem(itemId, numItems).asInstanceOf[ItemTO]
    //val idStr = itemIds.getOrElseUpdate(itemId, uuid())
    //itemSubjectDateTitleIndexInserts += Tuple2(
    //  ItemSubjectDateTitleIndexKey(to.getI_subject,
    //    to.getI_pub_date,
    //    to.getI_title),
    //  ItemKey(idStr))
    //val author =  authors.get(to.getI_a_id)

    //AuthorNameItemIndexInserts += Tuple2(AuthorNameItemIndexKey(author.get._2.A_FNAME, to.getI_title, idStr), NullRecord(true))
    //AuthorNameItemIndexInserts += Tuple2(AuthorNameItemIndexKey(author.get._2.A_LNAME, to.getI_title, idStr), NullRecord(true))

    //val tokens = to.getI_title.split("\\s+");

    //itemTitleIndexInserts ++= tokens.map(x => (ItemTitleIndexKey(x.toLowerCase, to.getI_title ,idStr), NullRecord(true)))

    (ItemKey(toItem(itemId)),
     ItemValue(
        to.getI_title,
        toAuthor(to.getI_a_id),
        to.getI_pub_date,
        to.getI_publisher,
        to.getI_subject,
        to.getI_desc,
        to.getI_related1,
        to.getI_related2,
        to.getI_related3,
        to.getI_related4,
        to.getI_related5,
        to.getI_thumbnail,
        to.getI_image,
        to.getI_srp,
        to.getI_cost,
        to.getI_avail,
        to.getI_stock,
        to.getI_isbn,
        to.getI_page,
        to.getI_backing,
        to.getI_dimensions
       )
      )
  }

  def createAuthorNameItemIndexes(item: (ItemKey, ItemValue)) : Seq[(AuthorNameItemIndexKey, NullRecord)] = {
    Seq(
      (AuthorNameItemIndexKey(toAuthorFname(fromAuthor(item._2.A_ID)), item._2.I_TITLE, item._1.I_ID), NullRecord(true)),
      (AuthorNameItemIndexKey(toAuthorLname(fromAuthor(item._2.A_ID)), item._2.I_TITLE, item._1.I_ID), NullRecord(true)))
  }

  def createItemSubjectDateTitleIndex(item: (ItemKey, ItemValue)) : (ItemSubjectDateTitleIndexKey, ItemKey) = {
    (ItemSubjectDateTitleIndexKey(item._2.I_SUBJECT, item._2.I_PUB_DATE, item._2.I_TITLE), item._1.copy())
  }

  def createItemTitleIndex(item: (ItemKey, ItemValue)) : Seq[(ItemTitleIndexKey, NullRecord)] = {
    item._2.I_TITLE.split("\\s+").map(token => {
      (ItemTitleIndexKey(token.toLowerCase, item._2.I_TITLE, item._1.I_ID), NullRecord(true))
    })
  }

  def createCountry(countryId : Int) : (CountryKey, CountryValue) = {
    val to = Generator.generateCountry(countryId).asInstanceOf[CountryTO]
    (CountryKey(countryId), CountryValue(to.getCo_name, to.getCo_exchange, to.getCo_currency))
  }

  def createXacts(orderId : Int) : (CcXactsKey,CcXactsValue) = {
    //val idStr = orderIds.getOrElseUpdate(orderId, uuid())
    val to = Generator.generateCCXacts(orderId).asInstanceOf[CCXactsTO]
    (CcXactsKey(toXact(orderId)),
     CcXactsValue(to.getCx_type,
      to.getCx_num,
      to.getCx_name,
      to.getCs_expiry,
      to.getCx_auth_id,
      to.getCx_xact_amt,
      to.getCx_xact_date,
      to.getCx_co_id))
  }

  def createAuthor(id : Int) : (AuthorKey, AuthorValue) = {
    val to = Generator.generateAuthor(id).asInstanceOf[AuthorTO]
    (AuthorKey(toAuthor(id)), AuthorValue(toAuthorFname(id), toAuthorLname(id), to.getA_mname, to.getA_dob, to.getA_bio))
    //var author = (AuthorKey(uuid()), AuthorValue(to.getA_fname, to.getA_lname, to.getA_mname, to.getA_dob, to.getA_bio))
    //authors += id ->  author
    //author
  }

  def createAddress(id : Int) : (AddressKey, AddressValue) = {
    val to = Generator.generateAddress(id).asInstanceOf[AddressTO]
    //val idStr = addressIds.getOrElseUpdate(id, uuid())
    (AddressKey(toAddress(id)),
    AddressValue( to.getAddr_street_1,
      to.getAddr_street_2,
      to.getAddr_city,
      to.getAddr_state,
      to.getAddr_zip,
      to.getAddr_co_id))
  }

  def createCustomer(id : Int) :  (CustomerKey, CustomerValue) = {
    val obj = Generator.generateCustomer( id , numCustomers )
    val to = obj.asInstanceOf[CustomerTO]
    (CustomerKey(toCustomer(to.getC_id)), //used naming convention instead of UUID
     CustomerValue(
       to.getC_passwd,
       to.getC_fname,
       to.getC_lname,
       toAddress(to.getC_addr_id),
       to.getC_phone.toString,
       to.getC_email,
       to.getC_since,
       to.getC_last_visit,
       to.getC_login,
       to.getC_expiration,
       to.getC_discount,
       to.getC_balance,
       to.getC_ytd_pmt,
       to.getC_birthday,
       to.getC_data)        
    )
  }
  

  def createOrder(id : Int) : (OrdersKey, OrdersValue) = {
    val obj = Generator.generateOrder(id, numCustomers, rand.nextInt(4) + 1)
    val to : OrderTO = obj.asInstanceOf[OrderTO]
    //val idStr = orderIds.getOrElseUpdate(id, uuid())
    //customerOrderIndexInserts += Tuple2(CustomerOrderIndex("cust" + to.getO_c_id, to.getO_date, idStr), NullRecord(true))
    (OrdersKey(toOrder(id)), OrdersValue(
      toCustomer(to.getO_c_id),
      to.getO_date,
      to.getO_sub_total,
      to.getO_tax,
      to.getO_total,
      to.getO_ship_type,
      to.getO_ship_date,
      toAddress(to.getO_bill_addr_id),
      toAddress(to.getO_ship_addr_id),
      to.getO_status))
  }

  def createCustomerOrderIndex(order: (OrdersKey, OrdersValue)) : (CustomerOrderIndex, NullRecord) = {
    (CustomerOrderIndex(order._2.O_C_ID, order._2.O_DATE_Time, order._1.O_ID), NullRecord(true))
  }

  def createOrderline(id : Int) : Seq[(OrderLineKey, OrderLineValue)] = {
    val orders : Seq[OrderLineTO] = (1 to rand.nextInt(4) + 1).map( Generator.generateOrderLine(id,_, numItems).asInstanceOf[OrderLineTO])
    for( (order, idx) <- orders.zipWithIndex) yield {
      (OrderLineKey(toOrder(id), idx + 1),
       OrderLineValue(toItem(order.getOl_i_id),
         order.getOl_qty,
         order.getOl_discount, 
         order.getOl_comments))
    }
  }

}

