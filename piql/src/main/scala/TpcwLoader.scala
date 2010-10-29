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

	val numCustomers : Int = (numEBs * 2880).intValue
	val numAddresses : Int = 2 * numCustomers
	val numAuthors : Int = (.25 * numItems).intValue
	val numOrders : Int = (.9 * numCustomers).intValue
  val numCountries : Int = 92

  val rand = new scala.util.Random

  private def uuid() : String = 
    UUID.randomUUID.toString

  private def nameUuid(s: String) : String = 
    UUID.nameUUIDFromBytes(s.getBytes("UTF-8")).toString

  /**
   * Given a cluster size, create the hex splits, sorted in string lexicographical order
   */
  private def hexSplit(clusterSize: Int) : Seq[Option[String]] = {
    var size = 16
    while (size < clusterSize)
      size *= 16
    val numPerNode = size.toDouble / clusterSize.toDouble
    assert(numPerNode >= 1.0)
    None +: (1 until clusterSize).map(i => ("%x".format((i.toDouble * numPerNode).toInt))).sorted.map(Some(_))
  }

  /**
   * Given a cluster size, create 8-bit ASCII split, sorted in string
   * lexicographical order
   */
  private def utf8Split(clusterSize: Int) = {
    var size = 256 
    while (size < clusterSize)
      size = size * 256 

    val numPerNode = size / clusterSize
    assert(numPerNode >= 1)

    // encodes i as a base 256 string. not super efficient
    def toKeyString(i: Int): String = i match {
      case 0 => ""
      case _ => toKeyString(i / 256) + (i % 256).toChar 
    }   

    None +: (1 until clusterSize).map(i => (toKeyString(i * numPerNode))).sorted.map(Some(_))
  }

  /**
   * Splits a given range [0, rangeSplit) between clusterSize. if
   * clusterSize exceeds the given range, then clusterSize - givenRange
   * elements are dropped
   */
  def rangeSplit(end: Int, clusterSize: Int): Seq[Option[Int]] = {
    val realSize = clusterSize - scala.math.max(clusterSize - end, 0)
    val numPerNode = end / realSize
    None +: (1 until realSize).map(i => Some(i * numPerNode))
  }

  def toCustomer(id: Int) = 
    nameUuid("cust%d".format(id))

  def toAuthor(id: Int) =
    nameUuid("author%d".format(id))

  /**
   * Author FNAME is generated from taking the author UUID and running
   * through another UUID
   */
  def toAuthorFname(authorId: String) =
    nameUuid("author_fname_%s".format(authorId))

  /**
   * Author LNAME is generated from taking the author UUID and running
   * through another UUID
   */
  def toAuthorLname(authorId: String) =
    nameUuid("author_lname_%s".format(authorId))

  def toAddress(id: Int) =
    nameUuid("address%d".format(id))

  def toXact(id: Int) = 
    nameUuid("xact%d".format(id))

  def toItem(id: Int) =
    nameUuid("item%d".format(id))

  def toOrder(id: Int) =
    nameUuid("order%d".format(id))

  def keySplits: TpcwKeySplits = {
    val servers = client.cluster.getAvailableServers
    val clusterSize = servers.size

    val hexSplits = hexSplit(clusterSize) zip servers
    val utf8Splits = utf8Split(clusterSize) zip servers

    // assume no replication factor
    TpcwKeySplits(
      // addresses
      hexSplits.map(x => (x._1.map(AddressKey(_)), List(x._2))),

      // authors
      hexSplits.map(x => (x._1.map(AuthorKey(_)), List(x._2))),

      // authorname_item_indexes
      hexSplits.map(x => (x._1.map(AuthorNameItemIndexKey(_, "", "")), List(x._2))),

      // xacts
      hexSplits.map(x => (x._1.map(CcXactsKey(_)), List(x._2))),

      // countries
      (rangeSplit(numCountries, clusterSize) zip servers).map(x => (x._1.map(CountryKey(_)), List(x._2))),

      // customers
      hexSplits.map(x => (x._1.map(CustomerKey(_)), List(x._2))),

      // items
      hexSplits.map(x => (x._1.map(ItemKey(_)), List(x._2))),

      // item_subject_date_title_indexes
      utf8Splits.map(x => (x._1.map(ItemSubjectDateTitleIndexKey(_, 0L, "")), List(x._2))),

      // orderlines
      hexSplits.map(x => (x._1.map(OrderLineKey(_, 0)), List(x._2))), 

      // orders
      hexSplits.map(x => (x._1.map(OrdersKey(_)), List(x._2))),

      // customer_indexes
      hexSplits.map(x => (x._1.map(CustomerOrderIndex(_, 0, "")), List(x._2))),

      // title_indexes
      utf8Splits.map(x => (x._1.map(ItemTitleIndexKey(_, "", "")), List(x._2))),

      // shopping_carts
      hexSplits.map(x => (x._1.map(ShoppingCartItemKey(_, "")), List(x._2)))
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
    customer_index: Seq[(Option[CustomerOrderIndex], List[StorageService])],
    title_index : Seq[(Option[ItemTitleIndexKey], List[StorageService])],
    shopping_cart : Seq[(Option[ShoppingCartItemKey], List[StorageService])]
    )


  def createNamespaces() = {
    val splits = keySplits
    client.cluster.createNamespace[AddressKey, AddressValue]("address", splits.address)
    client.cluster.createNamespace[AuthorKey, AuthorValue]("author", splits.author)
    client.cluster.createNamespace[AuthorNameItemIndexKey, NullRecord]("author_name_index", splits.authorname_item_index)
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
      (AuthorNameItemIndexKey(toAuthorFname(item._2.A_ID), item._2.I_TITLE, item._1.I_ID), NullRecord(true)),
      (AuthorNameItemIndexKey(toAuthorLname(item._2.A_ID), item._2.I_TITLE, item._1.I_ID), NullRecord(true)))
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
    (AuthorKey(toAuthor(id)), AuthorValue(toAuthorFname(toAuthor(id)), toAuthorLname(toAuthor(id)), to.getA_mname, to.getA_dob, to.getA_bio))
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

