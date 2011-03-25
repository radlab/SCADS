package edu.berkeley.cs
package scads
package piql
package tpcw

import net.lag.logging._

import edu.berkeley.cs.scads.comm._
import edu.berkeley.cs.scads.storage._
import edu.berkeley.cs.avro.marker._

import edu.berkeley.cs.scads.piql.DataGenerator._
import ch.ethz.systems.tpcw.populate.data.Generator
import java.util.UUID
import ch.ethz.systems.tpcw.populate.data.objects._
import collection.mutable.{ArrayBuffer, HashMap}
import org.apache.avro.generic.GenericRecord


class TpcwLoader( val client : TpcwClient,
                  val numClients: Int, // Number of LOADING clients
                  val numEBs : Double,
                  val numItems : Int ) {

  private val logger = Logger("edu.berkeley.cs.scads.piql.TpcwLoader")

  require(client != null)
  require(numClients >= 1)
  require(numEBs > 0.0)
  require(numItems > 0)

  /**
   * See clause 4.2.2 - items MUST be chosen from the following set
   */
  val ValidItemCardinalities = Set(1000, 10000, 100000, 1000000, 10000000)

  if (!ValidItemCardinalities.contains(numItems))
    logger.warning("%d is NOT a valid number of items for a TPC-W benchmark", numItems)

  /** Cardinalities defined by clause 4.3 */

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
    var levels = 1
    while (size < clusterSize) {
      size *= 16
      levels += 1
    }
    size *= 16; levels += 1 // go up one more level than required to get finer granularity
    val numPerNode = size.toDouble / clusterSize.toDouble
    assert(numPerNode >= 1.0)
    None +: (1 until clusterSize).map(i => (("%0" + levels + "x").format((i.toDouble * numPerNode).toInt))).sorted.map(Some(_))
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
   * Given a cluster size, create splits over printable chars, defined as
   * ASCII [33-126]
   */
  private def printableCharSplit(clusterSize: Int) = {
    val (low, high) = (33, 126)
    val numChars = high - low + 1
    var size = numChars
    var levels = 1
    while (size < clusterSize) {
      size *= numChars
      levels += 1
    }
    size *= numChars; levels += 1 // go up one more level than required to get finer granularity

    def toKeyString(i: Int): String =
      if (i < numChars) (i + low).toChar.toString
      else toKeyString(i / numChars) + ((i % numChars) + low).toChar.toString

    def pad(str: String, ch: Char, level: Int): String = {
      var s = str
      while (s.length < level) {
        s = ch.toString + s
      }
      s
    }

    val numPerNode = size.toDouble / clusterSize.toDouble
    assert(numPerNode >= 1.0)
    None +: (1 until clusterSize).map(i => pad(toKeyString((i.toDouble * numPerNode).toInt), low.toChar, levels)).sorted.map(Some(_))
  }

  /**
   * Splits a given range [0, rangeSplit) between clusterSize. if
   * clusterSize exceeds the given range, then clusterSize - givenRange
   * elements are dropped
   */
  private def rangeSplit(end: Int, clusterSize: Int): Seq[Option[Int]] = {
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
    val allServerSplitKey = List[(Option[GenericRecord], Seq[StorageService])]((None, client.cluster.getAvailableServers)  )


    TpcwKeySplits(allServerSplitKey,
      allServerSplitKey,
      allServerSplitKey,
      allServerSplitKey,
      allServerSplitKey,
      allServerSplitKey,
      allServerSplitKey,
      allServerSplitKey,
      allServerSplitKey,
      allServerSplitKey,
      allServerSplitKey,
      allServerSplitKey,
      allServerSplitKey)

//
//    val servers = client.cluster.getAvailableServers
//    val clusterSize = servers.size
//
//    val hexSplits = hexSplit(clusterSize) zip servers
//    val printableCharSplits = printableCharSplit(clusterSize) zip servers
//
//    // use sampling to create splits for ItemSubjectDateTitleIndexKey
//    // use 1000x the number of available servers as the number of samples to
//    // take
//
//    val itemSubjSamples = (1 to 1000 * clusterSize)
//      .map(_ => rand.nextInt(numItems) + 1)
//      .map(i => createItemSubjectDateTitleIndex(createItem(i)))
//      .sortWith { case ((ItemSubjectDateTitleIndexKey(x1, x2, _), _), (ItemSubjectDateTitleIndexKey(y1, y2, _), _)) =>
//        x1 < y1 || (x1 == y1 && x2 < y2)
//      }.toIndexedSeq
//
//    val itemSubDateTitleIndexSplits =
//      None +: (1 until clusterSize).map(i => Some(itemSubjSamples(i * 1000)._1))
//
//    logger.info("itemSubDateTitleIndexSplits: %s", itemSubDateTitleIndexSplits)
//
//    val itemTitleSamples = (1 to 1000 * clusterSize)
//      .map(_ => rand.nextInt(numItems) + 1)
//      .flatMap(i => createItemTitleIndex(createItem(i)))
//      .sortWith { case ((ItemTitleIndexKey(x1, x2, _), _), (ItemTitleIndexKey(y1, y2, _), _)) =>
//        x1 < y1 || (x1 == y1 && x2 < y2)
//      }.toIndexedSeq
//
//    val stepSize = itemTitleSamples.size.toDouble / clusterSize.toDouble
//    assert(stepSize > 0.0)
//
//    val itemTitleIndexSplits =
//      None +: (1 until clusterSize).map(i => Some(itemTitleSamples((i.toDouble * stepSize).toInt)._1))
//
//    // assume no replication factor
//    TpcwKeySplits(
//      // addresses
//      hexSplits.map(x => (x._1.map(AddressKey(_)), List(x._2))),
//
//      // authors
//      hexSplits.map(x => (x._1.map(AuthorKey(_)), List(x._2))),
//
//      // authorname_item_indexes
//      hexSplits.map(x => (x._1.map(AuthorNameItemIndexKey(_, "", "")), List(x._2))),
//
//      // xacts
//      hexSplits.map(x => (x._1.map(CcXactsKey(_)), List(x._2))),
//
//      // countries
//      (rangeSplit(numCountries, clusterSize) zip servers).map(x => (x._1.map(CountryKey(_)), List(x._2))),
//
//      // customers
//      hexSplits.map(x => (x._1.map(CustomerKey(_)), List(x._2))),
//
//      // items
//      hexSplits.map(x => (x._1.map(ItemKey(_)), List(x._2))),
//
//      // item_subject_date_title_indexes
//      (itemSubDateTitleIndexSplits zip servers).map(x => (x._1, List(x._2))),
//
//      // orderlines
//      hexSplits.map(x => (x._1.map(OrderLineKey(_, 0)), List(x._2))),
//
//      // orders
//      hexSplits.map(x => (x._1.map(OrdersKey(_)), List(x._2))),
//
//      // customer_indexes
//      hexSplits.map(x => (x._1.map(CustomerOrderIndex(_, 0, "")), List(x._2))),
//
//      // title_indexes
//      (itemTitleIndexSplits zip servers).map(x => (x._1, List(x._2))),
//
//      // shopping_carts
//      hexSplits.map(x => (x._1.map(ShoppingCartItemKey(_, "")), List(x._2)))
//    )
  }

  case class TpcwKeySplits(
    address: Seq[(Option[GenericRecord], Seq[StorageService])],
    author: Seq[(Option[GenericRecord], Seq[StorageService])],
    authorname_item_index: Seq[(Option[GenericRecord], Seq[StorageService])],
    xacts: Seq[(Option[GenericRecord], Seq[StorageService])],
    country: Seq[(Option[GenericRecord], Seq[StorageService])],
    customer: Seq[(Option[GenericRecord], Seq[StorageService])],
    item: Seq[(Option[GenericRecord], Seq[StorageService])],
    item_subject_date_title_index: Seq[(Option[GenericRecord], Seq[StorageService])],
    orderline: Seq[(Option[GenericRecord], Seq[StorageService])],
    orders: Seq[(Option[GenericRecord], Seq[StorageService])],
    customer_index: Seq[(Option[GenericRecord], Seq[StorageService])],
    title_index : Seq[(Option[GenericRecord], Seq[StorageService])],
    shopping_cart : Seq[(Option[GenericRecord], Seq[StorageService])]
    )


  def createNamespaces() = {
    println("Create Addresses")
    val splits  = keySplits
    val address = client.cluster.createNamespace[Address]("addresses", splits.address)
    println("Create Authors")
    val author = client.cluster.createNamespace[Author]("authors", splits.author)
    val authorFNameIdx = author.getOrCreateIndex("A_FNAME" :: Nil)
    //authorFNameIdx.setPartitionScheme(splits.authorname_item_index.map { case (optRec, seq) => (optRec.map(author.keyToBytes(_)), seq) } )

    val authorLNameIdx = author.getOrCreateIndex("A_LNAME" :: Nil)
    //authorFNameIdx.setPartitionScheme(splits.authorname_item_index.map { case (optRec, seq) => (optRec.map(author.keyToBytes(_)), seq) } )

    val xacts =  client.cluster.createNamespace[CcXact]("xacts", splits.xacts)

    val country = client.cluster.createNamespace[Country]("countries", splits.country)
    println("Create Customers")
    val customer = client.cluster.createNamespace[Customer]("customers", splits.customer)

    val item =  client.cluster.createNamespace[Item]("items", splits.item)
    val itemSubDateTitleIdx =  item.getOrCreateIndex("I_SUBJECT" :: "I_PUB_DATE" :: "I_TITLE" :: Nil)
    //itemSubDateTitleIdx.setPartitionScheme(splits.item_subject_date_title_index.map { case (optRec, seq) => (optRec.map(item.keyToBytes(_)), seq) } )
    val itemTitleIdx =   item.getOrCreateIndex("I_TITLE" :: Nil)
    //itemTitleIdx.setPartitionScheme(splits.title_index.map { case (optRec, seq) => (optRec.map(item.keyToBytes(_.toBytes)), seq) } )

    val orderline =   client.cluster.createNamespace[OrderLine]("orderLines", splits.orderline)

    val order =  client.cluster.createNamespace[Order]("orders", splits.orders)
    val custOrderIdx = order.getOrCreateIndex( "O_C_UNAME" :: "O_DATE_Time" :: Nil)
    //custOrderIcx.setPartitionScheme(splits.customer_index.map { case (optRec, seq) => (optRec.map(_.toBytes), seq) } )

    val cart = client.cluster.createNamespace[ShoppingCartItem]("shoppingCartItems", splits.shopping_cart)
  }

  case class TpcwData(
      // main objects
      addresses: Seq[Address],
      authors: Seq[Author],
      xacts: Seq[CcXact],
      countries: Seq[Country],
      customers: Seq[Customer],
      items: Seq[Item],
      orders: Seq[Order],
      orderlines: Seq[OrderLine]

      // secondary/inverted indicies
      //authorNameItemIndexes: Seq[(AuthorNameItemIndexKey, NullRecord)],
      //itemSubjectDateTitleIndexes: Seq[(ItemSubjectDateTitleIndexKey, ItemKey)],
      //customerOrderIndexes: Seq[(CustomerOrderIndex, NullRecord)],
      //itemTitleIndexes: Seq[(ItemTitleIndexKey, NullRecord)]
    ) {

    def load() = {
      client.addresses ++= addresses
      client.authors ++= authors
      client.xacts ++= xacts
      client.countries ++= countries
      client.customers ++= customers
      client.items ++= items
      client.orders ++= orders
      client.orderLines ++= orderlines

      //client.authorNameItemIndex ++= authorNameItemIndexes
      //client.itemSubjectDateTitleIndex ++= itemSubjectDateTitleIndexes
      //client.customerOrderIndex ++= customerOrderIndexes
      //client.itemTitleIndex ++= itemTitleIndexes
    }
  }

  /**
   * Assumes clientId is indexed by 0
   */
  def getData(clientId: Int, useViews: Boolean = true) : TpcwData = {
    require(0 <= clientId && clientId < numClients, "Invalid client id")

    /** log what the entire data set will look like in terms of sizes */
    logger.info("--- Entire TPC-W data set ---")
    logger.info("numCustomers: %d", numCustomers)
    logger.info("numAddresses: %d", numAddresses)
    logger.info("numAuthors: %d", numAuthors)
    logger.info("numOrders: %d", numOrders)
    logger.info("numCountries: %d", numCountries)
    logger.info("numItems: %d", numItems)

    /** assuming [1, upperBound], returns the slice of data for this clientId */
    def getSlice(upperBound: Int) = {
      require(upperBound >= 1)
      val numPerClient = upperBound / numClients
      if (numPerClient == 0) { // this is the case where there are more clients than elements to slice
        if (clientId >= upperBound) Seq.empty
        else (clientId + 1 to clientId + 1)
      } else {
        val start = clientId * numPerClient + 1
        if (clientId == numClients - 1) (start to upperBound)
        else (start until (start + numPerClient))
      }
    }

    def newRange(upperBound: Int) =
      if (useViews) getSlice(upperBound).view
      else getSlice(upperBound)

    val addresses = newRange(numAddresses).map(createAddress(_))
    val authors = newRange(numAuthors).map(createAuthor(_))
    val xacts = newRange(numOrders).map(createXacts(_))
    val countries = newRange(numCountries).map(createCountry(_))
    val customers = newRange(numCustomers).map(createCustomer(_))

    // these two can't be views b/c we need each invocation to be
    // deterministic
    val items = getSlice(numItems).map(createItem(_))
    val orders = getSlice(numOrders).map(createOrder(_))

    val orderlines = newRange(numOrders).flatMap(createOrderline(_))

    //val authorNameItemIndexes = items.flatMap(createAuthorNameItemIndexes(_))
    //val itemSubjectDateTitleIndexes = items.map(createItemSubjectDateTitleIndex(_))
    //val customerOrderIndexes = orders.map(createCustomerOrderIndex(_))
    //val itemTitleIndexes = items.flatMap(createItemTitleIndex(_))

    /** log what this client will be loading */
    logger.info("--- ClientId %d's Data Slice ---", clientId)
    logger.info("numCustomers: %d", getSlice(numCustomers).size)
    logger.info("numAddresses: %d", getSlice(numAddresses).size)
    logger.info("numAuthors: %d", getSlice(numAuthors).size)
    logger.info("numOrders: %d", getSlice(numOrders).size)
    logger.info("numCountries: %d", getSlice(numCountries).size)
    logger.info("numItems: %d", getSlice(numItems).size)

    TpcwData(
      addresses,
      authors,
      xacts,
      countries,
      customers,
      items,
      orders,
      orderlines
      //authorNameItemIndexes,
      //itemSubjectDateTitleIndexes,
      //customerOrderIndexes,
      //itemTitleIndexes
      )
  }

  def createItem(itemId : Int) : Item = {
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

    var item = Item(toItem(itemId))
    item.I_TITLE = to.getI_title
    item.A_ID = toAuthor(to.getI_a_id)
    item.I_PUB_DATE = to.getI_pub_date
    item.I_PUBLISHER = to.getI_publisher
    item.I_SUBJECT = to.getI_subject
    item.I_DESC = to.getI_desc
    item.I_RELATED1 = to.getI_related1
    item.I_RELATED2 = to.getI_related2
    item.I_RELATED3 = to.getI_related3
    item.I_RELATED4 = to.getI_related4
    item.I_RELATED5 = to.getI_related5
    item.I_THUMBNAIL = to.getI_thumbnail
    item.I_IMAGE = to.getI_image
    item.I_SRP = to.getI_srp
    item.I_COST = to.getI_cost
    item.I_AVAIL = to.getI_avail
    item.I_STOCK = to.getI_stock
    item.ISBN = to.getI_isbn
    item.I_PAGE = to.getI_page
    item.I_BACKING = to.getI_backing
    item.I_DIMENSION = to.getI_dimensions
    item
  }

//  def createAuthorNameItemIndexes(item: (ItemKey, ItemValue)) : Seq[(AuthorNameItemIndexKey, NullRecord)] = {
//    Seq(
//      (AuthorNameItemIndexKey(toAuthorFname(item._2.A_ID), item._2.I_TITLE, item._1.I_ID), NullRecord(true)),
//      (AuthorNameItemIndexKey(toAuthorLname(item._2.A_ID), item._2.I_TITLE, item._1.I_ID), NullRecord(true)))
//  }

//  def createItemSubjectDateTitleIndex(item: (ItemKey, ItemValue)) : (ItemSubjectDateTitleIndexKey, ItemKey) = {
//    (ItemSubjectDateTitleIndexKey(item._2.I_SUBJECT, item._2.I_PUB_DATE, item._2.I_TITLE), item._1.copy())
//  }

//  def createItemTitleIndex(item: (ItemKey, ItemValue)) : Seq[(ItemTitleIndexKey, NullRecord)] = {
//    item._2.I_TITLE.split("\\s+").map(token => {
//      (ItemTitleIndexKey(token.toLowerCase, item._2.I_TITLE, item._1.I_ID), NullRecord(true))
//    })
//  }

  def createCountry(countryId : Int) : Country = {
    val to = Generator.generateCountry(countryId).asInstanceOf[CountryTO]
    var country = Country(countryId)
    country.CO_NAME = to.getCo_name
    country.CO_EXCHANGE = to.getCo_exchange
    country.CO_CURRENCY =  to.getCo_currency
    country
  }

  def createXacts(orderId : Int) : CcXact = {
    //val idStr = orderIds.getOrElseUpdate(orderId, uuid())
    val to = Generator.generateCCXacts(orderId).asInstanceOf[CCXactsTO]
    var xact = CcXact(toXact(orderId))
    xact.CX_TYPE = to.getCx_type
    xact.CX_NUM = to.getCx_num
    xact.CX_NAME = to.getCx_name
    xact.CX_EXPIRY = to.getCs_expiry
    xact.CX_AUTH_ID = to.getCx_auth_id
    xact.CX_XACT_AMT = to.getCx_xact_amt
    xact.CX_XACT_DATE = to.getCx_xact_date
    xact.CX_CO_ID = to.getCx_co_id
    xact
  }

  def createAuthor(id : Int) : Author = {
    val to = Generator.generateAuthor(id).asInstanceOf[AuthorTO]

    var author = Author(toAuthor(id))
    author.A_FNAME = toAuthorFname(toAuthor(id))
    author.A_LNAME = toAuthorLname(toAuthor(id))
    author.A_MNAME = to.getA_mname
    author.A_DOB = to.getA_dob
    author.A_BIO = to.getA_bio

    author

    //var author = (AuthorKey(uuid()), AuthorValue(to.getA_fname, to.getA_lname, to.getA_mname, to.getA_dob, to.getA_bio))
    //authors += id ->  author
    //author
  }

  def createAddress(id : Int) : Address = {
    val to = Generator.generateAddress(id).asInstanceOf[AddressTO]
    //val idStr = addressIds.getOrElseUpdate(id, uuid())
    var address = Address(toAddress(id))
    address.ADDR_STREET1 =  to.getAddr_street_1
    address.ADDR_STREET2 =  to.getAddr_street_2
    address.ADDR_CITY =  to.getAddr_city
    address.ADDR_STATE =  to.getAddr_state
    address.ADDR_ZIP =  to.getAddr_zip
    address.ADDR_CO_ID =  to.getAddr_co_id
    address
  }

  def createCustomer(id : Int) :  Customer = {
    val to = Generator.generateCustomer( id , numCustomers ).asInstanceOf[CustomerTO]
    var customer = Customer(toCustomer(to.getC_id))   //used naming convention instead of UUID
    customer.C_PASSWD = to.getC_passwd
    customer.C_FNAME = to.getC_fname
    customer.C_LNAME = to.getC_lname
    customer.C_ADDR_ID = toAddress(to.getC_addr_id)
    customer.C_PHONE = to.getC_phone.toString
    customer.C_EMAIL = to.getC_email
    customer.C_SINCE = to.getC_since
    customer.C_LAST_VISIT = to.getC_last_visit
    customer.C_LOGIN = to.getC_login
    customer.C_EXPIRATION = to.getC_expiration
    customer.C_DISCOUNT = to.getC_discount
    customer.C_BALANCE = to.getC_balance
    customer.C_YTD_PMT = to.getC_ytd_pmt
    customer.C_BIRTHDATE = to.getC_birthday
    customer.C_DATA = to.getC_data
    customer
  }


  def createOrder(id : Int) : Order = {
    val to = Generator.generateOrder(id, numCustomers, rand.nextInt(4) + 1).asInstanceOf[OrderTO]
    //val idStr = orderIds.getOrElseUpdate(id, uuid())
    //customerOrderIndexInserts += Tuple2(CustomerOrderIndex("cust" + to.getO_c_id, to.getO_date, idStr), NullRecord(true))
    var order = Order(toOrder(id))
    order.O_C_UNAME = toCustomer(to.getO_c_id)
    order.O_DATE_Time = to.getO_date
    order.O_SUB_TOTAL = to.getO_sub_total
    order.O_TAX = to.getO_tax
    order.O_TOTAL = to.getO_total
    order.O_SHIP_TYPE = to.getO_ship_type
    order.O_SHIP_DATE = to.getO_ship_date
    order.O_BILL_ADDR_ID = toAddress(to.getO_bill_addr_id)
    order.O_SHIP_ADDR_ID = toAddress(to.getO_ship_addr_id)
    order.O_STATUS = to.getO_status
    order
  }

//  def createCustomerOrderIndex(order: (OrdersKey, OrdersValue)) : (CustomerOrderIndex, NullRecord) = {
//    (CustomerOrderIndex(order._2.O_C_ID, order._2.O_DATE_Time, order._1.O_ID), NullRecord(true))
//  }


  def createOrderline(id : Int) : Seq[OrderLine] = {
    val orders : Seq[OrderLineTO] = (1 to rand.nextInt(4) + 1).map( Generator.generateOrderLine(id,_, numItems).asInstanceOf[OrderLineTO])

    for( (order, idx) <- orders.zipWithIndex) yield {
      var orderline = OrderLine(toOrder(id), idx + 1)
      orderline.OL_I_ID = toItem(order.getOl_i_id)
      orderline.OL_QTY = order.getOl_qty
      orderline.OL_DISCOUNT = order.getOl_discount
      orderline.OL_COMMENT =   order.getOl_comments
      orderline
    }
  }

}
