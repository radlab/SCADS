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
import org.apache.avro.generic.IndexedRecord
import edu.berkeley.cs.avro.runtime._
import org.apache.avro.util.Utf8


class TpcwLoader( val client : TpcwClient,
                  val numClients: Int, // Number of LOADING clients
                  val numEBs : Double,
                  val numItems : Int,
                  val replicationFactor: Int ) {

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


  def keySplits(clusterSize: Int): TpcwTableSplits = {
    val hexSplits = hexSplit(clusterSize)
    val printableCharSplits = printableCharSplit(clusterSize)

    // assume no replication factor
    TpcwTableSplits(
      // addresses
      hexSplits.map(_.map(Address(_).key)),

      // authors
      hexSplits.map(_.map(Author(_).key)),

      // xacts
      hexSplits.map(_.map(CcXact(_).key)),

      // countries
      (rangeSplit(numCountries, clusterSize)).map(_.map(Country(_).key)),

      // customers
      hexSplits.map(_.map(Customer(_).key)),

      // items
      hexSplits.map(_.map(Item(_).key)),

      // orderlines
      hexSplits.map(_.map(OrderLine(_, 0).key)),

      // orders
      hexSplits.map(_.map(Order(_).key)),

      // shopping_carts
      hexSplits.map(_.map(ShoppingCartItem(_, "").key))
    )
  }

  case class TpcwTableSplits(
    address: Seq[Option[IndexedRecord]],
    author: Seq[Option[IndexedRecord]],
    xacts: Seq[Option[IndexedRecord]],
    country: Seq[Option[IndexedRecord]],
    customer: Seq[Option[IndexedRecord]],
    item: Seq[Option[IndexedRecord]],
    orderline: Seq[Option[IndexedRecord]],
    orders: Seq[Option[IndexedRecord]],
    shopping_cart : Seq[Option[IndexedRecord]]
  )


  def indexSplits(authorFNameIdx : IndexNamespace, itemSubDateTitleIdx : IndexNamespace, itemTitleIdx : IndexNamespace, custOrderIdx : IndexNamespace) : TpcwIndexSplits = {
     // use sampling to create splits for ItemSubjectDateTitleIndexKey
    // use 1000x the number of available servers as the number of samples to
    // take
    val servers = client.cluster.getAvailableServers
    val clusterSize = servers.size
    val hexSplits = hexSplit(clusterSize) zip servers

    val itemSubjSamples = (1 to 1000 * clusterSize)
      .map(_ => rand.nextInt(numItems) + 1)
      .map(i => createItemSubjectDateTitleIndex(itemSubDateTitleIdx, createItem(i)))
      .sortWith { case (a : IndexedRecord, b : IndexedRecord) => a < b
      }.toIndexedSeq

    val itemSubDateTitleIndexSplits =
      None +: (1 until clusterSize).map(i => Some(itemSubjSamples(i * 1000)))

    logger.info("itemSubDateTitleIndexSplits: %s", itemSubDateTitleIndexSplits)

    val itemTitleSamples = (1 to 1000 * clusterSize)
      .map(_ => rand.nextInt(numItems) + 1)
      .flatMap(i => createItemTitleIndex(itemTitleIdx, createItem(i)))
      .sortWith {  case (a : IndexedRecord, b : IndexedRecord) => a < b
      }.toIndexedSeq

    val stepSize = itemTitleSamples.size.toDouble / clusterSize.toDouble
      assert(stepSize > 0.0)

    val itemTitleIndexSplits =
      None +: (1 until clusterSize).map(i => Some(itemTitleSamples((i.toDouble * stepSize).toInt)))

    TpcwIndexSplits(
      // authorname_item_indexes
      hexSplits.map(x => (x._1.map(createString2Split(authorFNameIdx, _)), List(x._2))),
      // item_subject_date_title_indexes
      (itemSubDateTitleIndexSplits zip servers).map(x => (x._1, List(x._2))),
      // customer_indexes
      hexSplits.map(x => (x._1.map(createCustOrderIndexes(custOrderIdx, _)), List(x._2))),
      // title_indexes
      (itemTitleIndexSplits zip servers).map(x => (x._1, List(x._2)))
    )

  }

  case class TpcwIndexSplits(
    authorname_item_index: Seq[(Option[IndexedRecord], Seq[StorageService])],
    item_subject_date_title_index: Seq[(Option[IndexedRecord], Seq[StorageService])],
    customer_index: Seq[(Option[IndexedRecord], Seq[StorageService])],
    title_index : Seq[(Option[IndexedRecord], Seq[StorageService])]
  )

  def createString2Split(idx : IndexNamespace,  split : String) : IndexedRecord = {
    var key = idx.newKeyInstance
    key.put(0, new Utf8(split))
    key.put(1, new Utf8(""))
    key
  }

  def createCustOrderIndexes(idx : IndexNamespace,  split : String) : IndexedRecord = {
    var key = idx.newKeyInstance
    key.put(0, new Utf8(split))
    key.put(1, 0L)
    key.put(2, new Utf8(""))
    key
  }

  def createAuthorNameItemIndexes(idx : IndexNamespace, item: Item) : Seq[IndexedRecord] = {
    var key1 = idx.newKeyInstance
    key1.put(0, new Utf8(toAuthorFname(item.A_ID) ))
    key1.put(1, new Utf8(item.I_TITLE) )
    key1.put(2, new Utf8(item.I_ID ) )
    var key2 = idx.newKeyInstance
    key2.put(0, new Utf8(toAuthorLname(item.A_ID) ))
    key2.put(1, new Utf8(item.I_TITLE ))
    key2.put(2, new Utf8(item.I_ID ))
    Seq( key1, key2)
  }

  def createItemSubjectDateTitleIndex(idx : IndexNamespace, item: Item) : IndexedRecord  = {
    var key = idx.newKeyInstance
    key.put(0, new Utf8(item.I_SUBJECT ))
    key.put(1, item.I_PUB_DATE )
    key.put(2, new Utf8(item.I_TITLE ))
    key.put(3, new Utf8(item.I_ID ) )
    key
  }

  def createItemTitleIndex(idx : IndexNamespace, item: Item) : Seq[IndexedRecord] = {
    var key = idx.newKeyInstance

    item.I_TITLE.split("\\s+").map(token => {
      var key = idx.newKeyInstance
      key.put(0, new Utf8(token.toLowerCase ))
      //key.put(1, item.I_TITLE )
      key.put(1, new Utf8(item.I_ID) )
      key
    })
  }

  //HACK: this should be in the namespace somewhere
  protected def createNamespace(namespace: PairNamespace[AvroPair], keySplits: Seq[Option[IndexedRecord]]): Unit = {
    val services = namespace.cluster.getAvailableServers.grouped(replicationFactor).toSeq
    val partitionScheme = keySplits.map(_.map(namespace.keyToBytes)).zip(services)
    namespace.setPartitionScheme(partitionScheme)
  }

  def createNamespaces(cluster: ScadsCluster) = {
    implicit def toGenericNs[A <: AvroPair](a: PairNamespace[A]) = a.asInstanceOf[PairNamespace[AvroPair]] //HACK: fix variance
    logger.info("Create Addresses")

    val servers = cluster.getAvailableServers
    val splits  = keySplits(servers.size)
    val replicaGroups = servers.grouped(replicationFactor).toSeq

    createNamespace(client.addresses, splits.address)
    createNamespace(client.authors, splits.author)
    createNamespace(client.xacts, splits.xacts)
    createNamespace(client.countries, splits.country)
    createNamespace(client.customers, splits.customer)
    createNamespace(client.items, splits.item)
    createNamespace(client.orderLines, splits.orderline)
    createNamespace(client.orders, splits.orders)
    createNamespace(client.shoppingCartItems, splits.shopping_cart)


    // Indexes
//    val idxSplits = indexSplits(authorFNameIdx, itemSubDateTitleIdx, itemTitleIdx, custOrderIdx)
//    createNamespace(client.authors.getOrCreateIndex("A_FNAME" :: Nil), idxSplits.authorname_item_index)
//    createNamespace(client.items.getOrCreateIndex("I_SUBJECT" :: "I_PUB_DATE" :: "I_TITLE" :: Nil), idxSplits.item_subject_date_title_index)
//    createNamespace(client.items.getOrCreateIndex("I_TITLE" :: Nil), idxSplits.title_index)
//    createNamespace(client.orders.getOrCreateIndex( "O_C_UNAME" :: "O_DATE_Time" :: Nil), idxSplits.customer_index)
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
