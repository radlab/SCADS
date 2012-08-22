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

import scala.actors.Future
import scala.actors.Futures._

class TpcwLoader(val numEBs : Double,
                 val numItems : Int) {
  private val logger = Logger()

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

  protected val rand = new scala.util.Random

  trait NamespaceData {
    type Pair <: AvroPair
    protected val ns: PairNamespace[Pair]

    protected def sampleData: Seq[Pair]
    protected def dataSlice(clientId: Int, numClients: Int): Seq[Pair]

    def repartition(replicationFactor: Int): Unit =
      ns.repartition(sampleData, replicationFactor)

    def repartitionForClusters(numClusters: Int): Unit =
      ns.repartitionForClusters(sampleData, numClusters)

    def load(clientId: Int = 0, numLoaders: Int = 1): Unit =
      ns ++=  dataSlice(clientId, numLoaders)

        /** assuming [1, upperBound], returns the slice of data for this clientId */
    protected def getSlice(clientId: Int, numClients: Int, upperBound: Int) = {
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
  }

  trait NoLoad extends NamespaceData {
    override def dataSlice(clientId: Int, numClients: Int) = Nil
  }

  case class RandomData[A <: AvroPair](ns: PairNamespace[A], generator: Int => A, numRecords: Int) extends NamespaceData {
    type Pair = A
    def sampleData = (1 to numRecords).view.map(generator)
    def dataSlice(clientId: Int, numClients: Int) = getSlice(clientId, numClients, numRecords).view.map(generator)
  }

  case class FlattenedRandomData[A <: AvroPair](ns: PairNamespace[A], generator: Int => Seq[A], numRecords: Int) extends NamespaceData {
    type Pair = A
    def sampleData = (1 to numRecords).view.flatMap(generator)
    def dataSlice(clientId: Int, numClients: Int) = getSlice(clientId, numClients, numRecords).view.flatMap(generator)
  }

  //TODO: Partition shopping cart
  def namespaces(client: TpcwClient): Seq[NamespaceData] =
    List[NamespaceData](
      RandomData(client.addresses, createAddress(_), numCustomers),
      RandomData(client.authors, createAuthor(_), numAuthors),
      RandomData(client.xacts, createXacts(_), numOrders),
      RandomData(client.countries, createCountry(_), numCountries),
      RandomData(client.customers, createCustomer(_), numCustomers),
      RandomData(client.items, createItem(_), numItems),
      FlattenedRandomData(client.orderLines, createOrderline(_), numOrders),
      RandomData(client.orders, createOrder(_), numOrders),
      new RandomData(client.shoppingCartItems, i => ShoppingCartItem(toCustomer(i), ""), numCustomers) with NoLoad)

  def createNamespaces(client: TpcwClient, replicationFactor: Int) =
    namespaces(client).foreach(_.repartition(replicationFactor))

  def createNamespacesForClusters(client: TpcwClient, numClusters: Int) = {
    namespaces(client).map(n => future {n.repartitionForClusters(numClusters)}).map(_())
  }

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
    item.I_A_ID = toAuthor(to.getI_a_id)
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
