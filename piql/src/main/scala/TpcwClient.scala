package edu.berkeley.cs.scads.piql

import edu.berkeley.cs.scads.storage._
import edu.berkeley.cs.avro.marker._

import org.apache.avro.util._

class TpcwClient(cluster: ScadsCluster, executor: QueryExecutor) {
    val address = cluster.getNamespace[AddressKey, AddressValue]("address")
    val author = cluster.getNamespace[AuthorKey, AuthorValue]("author")
    val authorFNameIndex = cluster.getNamespace[AuthorFNameIndexKey, NullRecord]("author_fname_index")
    val authorLNameIndex = cluster.getNamespace[AuthorLNameIndexKey, NullRecord]("author_lname_index")
    val xacts = cluster.getNamespace[CcXactsKey, CcXactsValue]("xacts")
    val country = cluster.getNamespace[CountryKey, CountryValue]("country")
    val customer = cluster.getNamespace[CustomerKey, CustomerValue]("customer")
    val customerNameIndex = cluster.getNamespace[CustomerNameKey, CustomerKey]("customer_index")  //Extra index
    val item = cluster.getNamespace[ItemKey, ItemValue]("item")
    val itemSubjectDateTitleIndex = cluster.getNamespace[ItemSubjectDateTitleIndexKey, ItemKey]("item_subject_date_title_index")
    val itemAuthorTitleIndex = cluster.getNamespace[ItemSubjectDateTitleIndexKey, ItemKey]("item_author_title_index")
    val orderline = cluster.getNamespace[OrderLineKey, OrderLineValue]("orderline")
    val order = cluster.getNamespace[OrdersKey, OrdersValue]("orders")

    implicit def toGeneric(ns: SpecificNamespace[_, _]) = ns.genericNamespace

    val homeWIPlan = IndexLookupJoin(customer, Array(AttributeValue(0, 1)), IndexLookup(customerNameIndex, Array(ParameterValue(0))))
    def homeWI(username: String): QueryResult =
      exec(homeWIPlan, new Utf8(username))


    //TODO Still missing the like operator. Does the like really work here???
    val newProductPlan = IndexLookupJoin(
                            author,
                            Array(AttributeValue(0, 1)), 
                            IndexLookupJoin(
                               item,
                               Array(AttributeValue(0, 1)),
                               IndexScan(itemSubjectDateTitleIndex, Array(ParameterValue(0)), ))
                          )
    def newProductWI(subject: String): QueryResult =
      exec(newProductPlan, new Utf8(username))

    val productDetailPlan = IndexLookupJoin(
                               author,
                               Array(AttributeValue(0, 2)),
                               IndexLookup(item, Array(ParameterValue(0)))
                            )
    def productDetailWI(bookId: Int): QueryResult =
      exec(newProductPlan, bookId)

    def searchByAuthorPlan =

      Union(
        IndexLookup(authorFNameIndex, Array(ParameterValue(0))),
        IndexLookup(author, Array(ParameterValue(0))),
        AttributeValue(0,0))
  

    def exec(plan: QueryPlan, args: Any*) = {
      val iterator = executor(plan, args:_*)
      iterator.open
      iterator.toList
    }

  //AttributeValue does it start counting from 0 or 1???
  //Questions: Why das Attribute value take a record ID?
  //should we rename KeyGenerator to projection?
}