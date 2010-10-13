package edu.berkeley.cs.scads.piql

import edu.berkeley.cs.scads.comm._
import edu.berkeley.cs.scads.storage._
import edu.berkeley.cs.avro.marker._

import edu.berkeley.cs.scads.piql.DataGenerator._
import ch.ethz.systems.tpcw.populate.data.Generator
import java.util.UUID
import ch.ethz.systems.tpcw.populate.data.objects._
import collection.mutable.{ArrayBuffer, HashMap}

/**
 * Created by IntelliJ IDEA.
 * User: tim
 * Date: Oct 11, 2010
 * Time: 11:16:03 AM
 * To change this template use File | Settings | File Templates.
 */

class TpcwLoader(val client: TpcwClient,
                  val numEBs : Double,
                  val numItems : Int,
                  val numCustomers : Int,
                  val numAddresses : Int,
                  val numAuthors : Int,
                  val numOrders :Int){

  private var addressIds = new  HashMap[Int, String]()
	private var orderIds = new  HashMap[Int, String]()
	private var itemSubjects  = new  HashMap[Int, String]()
	private var orderDates = new  HashMap[Int, String]()
  private var scheduledOrderLines = new ArrayBuffer[(String, Int)]();

  val rand = new scala.util.Random(System.currentTimeMillis)

  private def uuid() : String = {
    UUID.randomUUID().toString();
  }

  private var ctr : Int = 10000

  private def id(c_name : String) : String = {
    ctr += 1
    if(ctr > 99999) ctr = 10000
    c_name + System.currentTimeMillis + ctr;
  }


  def keySplits: TpcwKeySplits = {
    val servers = client.cluster.getAvailableServers
    val clusterSize = servers.size
    return TpcwKeySplits(
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
    author_fname_index: Seq[(Option[AuthorFNameIndexKey], List[StorageService])],
    xacts: Seq[(Option[CcXactsKey], List[StorageService])],
    country: Seq[(Option[CountryKey], List[StorageService])],
    customer: Seq[(Option[CustomerKey], List[StorageService])],
    item: Seq[(Option[ItemKey], List[StorageService])],
    item_subject_date_title_index: Seq[(Option[ItemSubjectDateTitleIndexKey], List[StorageService])],
    orderline: Seq[(Option[OrderLineKey], List[StorageService])],
    orders: Seq[(Option[OrdersKey], List[StorageService])],
    customer_index: Seq[(Option[CustomerOrderIndex], List[StorageService])]
    )



  def createNamespaces() = {
    val splits = keySplits
    client.cluster.createNamespace[AddressKey, AddressValue]("address", splits.address)
    client.cluster.cluster.createNamespace[AuthorKey, AuthorValue]("author", splits.author)
    client.cluster.cluster.createNamespace[AuthorFNameIndexKey, NullRecord]("author_fname_index", splits.author_fname_index)
    //val authorLNameIndex = cluster.getNamespace[AuthorLNameIndexKey, NullRecord]("author_lname_index") //make it one
    client.cluster.cluster.createNamespace[CcXactsKey, CcXactsValue]("xacts", splits.xacts)
    client.cluster.cluster.createNamespace[CountryKey, CountryValue]("country", splits.country)
    client.cluster.cluster.createNamespace[CustomerKey, CustomerValue]("customer", splits.customer)
    client.cluster.cluster.createNamespace[ItemKey, ItemValue]("item", splits.item)
    client.cluster.cluster.createNamespace[ItemSubjectDateTitleIndexKey, ItemKey]("item_subject_date_title_index", splits.item_subject_date_title_index)
    client.cluster.cluster.createNamespace[OrderLineKey, OrderLineValue]("orderline", splits.orderline)
    client.cluster.cluster.createNamespace[OrdersKey, OrdersValue]("orders", splits.orders)
    client.cluster.cluster.createNamespace[CustomerOrderIndex, OrdersKey]("customer_index", splits.customer_index)  //Extra index
  }

  def load() = {

    client.order ++= (1 to numOrders).view.map(a => createOrder(Generator.generateOrder(a, numCustomers, rand.nextInt(4) + 1)))
  }


  def createOrder(obj : AbstractTO) : (OrdersKey, OrdersValue) = {
    val to : OrderTO = obj.asInstanceOf[OrderTO]
    val id : String = uuid
    orderIds += to.getO_id -> id
    (OrdersKey(uuid), OrdersValue(
      "user" + to.getO_c_id,
      to.getO_date,
      to.getO_sub_total,
      to.getO_tax,
      to.getO_total,
      to.getO_ship_type,
      to.getO_ship_date,
      addressIds.getOrElseUpdate(to.getO_bill_addr_id, uuid),
      addressIds.getOrElseUpdate(to.getO_ship_addr_id, uuid),
      to.getO_status))

  }

}

