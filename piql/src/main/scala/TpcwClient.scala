package edu.berkeley.cs.scads.piql

import edu.berkeley.cs.scads.storage._
import edu.berkeley.cs.avro.marker._

import org.apache.avro.util._

class TpcwClient(cluster: ScadsCluster, executor: QueryExecutor) {
    val address = cluster.getNamespace[AddressKey, AddressValue]("address")
    val author = cluster.getNamespace[AuthorKey, AuthorValue]("author")
    val authorNameIndex = cluster.getNamespace[AuthorFNameIndexKey, NullRecord]("author_fname_index")
    //val authorLNameIndex = cluster.getNamespace[AuthorLNameIndexKey, NullRecord]("author_lname_index") //make it one
    val xacts = cluster.getNamespace[CcXactsKey, CcXactsValue]("xacts")
    val country = cluster.getNamespace[CountryKey, CountryValue]("country")
    val customer = cluster.getNamespace[CustomerKey, CustomerValue]("customer")
    val item = cluster.getNamespace[ItemKey, ItemValue]("item")
    val itemSubjectDateTitleIndex = cluster.getNamespace[ItemSubjectDateTitleIndexKey, ItemKey]("item_subject_date_title_index")
    val itemAuthorTitleIndex = cluster.getNamespace[ItemSubjectDateTitleIndexKey, ItemKey]("item_author_title_index")
    val orderline = cluster.getNamespace[OrderLineKey, OrderLineValue]("orderline")
    val order = cluster.getNamespace[OrdersKey, OrdersValue]("orders")
    val customerOrderIndex = cluster.getNamespace[CustomerOrderIndex, OrdersKey]("customer_index")  //Extra index

    def paraSelector(i : Int) = Array(ParameterValue(i))
    def projection(record: Int, attribute: Int) = Array(AttributeValue(record, attribute))
    val firstPara = paraSelector(0)


    implicit def toGeneric(ns: SpecificNamespace[_, _]) = ns.genericNamespace

    val homeWIPlan = IndexLookup(customer, Array(ParameterValue(0)))
    def homeWI(username: String): QueryResult =
      exec(homeWIPlan, new Utf8(username))


    //TODO Still missing the like operator. Does the like really work here???
    val newProductPlan = IndexLookupJoin(  //(itemSubjectDateTitleIndex, itemKey, itemKey, ItemValue, authorKey, authorValue)
                            author,
                            projection(3, 1), //(itemSubjectDateTitleIndex, itemKey, itemKey, ItemValue)
                            IndexLookupJoin(
                               item,
                               projection(0, 1),
                               IndexScan(itemSubjectDateTitleIndex, firstPara, 50, true))
                          )
      def newProductWI(subject: String): QueryResult =
      exec(newProductPlan, new Utf8(username))

    val productDetailPlan = IndexLookupJoin( //(ItemKey, ItemValue, AuthorKey, AuthorValue)
                               author,
                               projection(1, 1), //(ItemKey, ItemValue)
                               IndexLookup(item, Array(ParameterValue(0)))
                            )
    def productDetailWI(bookId: Int): QueryResult =
      exec(newProductPlan, bookId)

    //TODO: Alternatively, no limit just pagination!!!
    val searchByAuthorPlan =
        Sort(
          projection(3,0), //(authorNameIndex, null, ItemKey, ItemValu)
          true,
          IndexLookupJoin(
            item,
            projection(0,1), //(authorNameIndex, null)
            IndexScan(authorNameIndex, firstPara, 50, true)
            )
          )
    def searchByAuthorWI(name : String) = exec(searchByAuthorPlan, name)

    val searchBySubjectPlan =
     IndexLookupJoin( // (I_SUBJECT, I_PUB_DATE, I_TITLE),(I_ID), Item, AuthorValue
      item,
      projection(2,1), // (I_SUBJECT, I_PUB_DATE, I_TITLE),(I_ID), ItemValue
      IndexLookupJoin( // (I_SUBJECT, I_PUB_DATE, I_TITLE),(I_ID), ItemValue
        item,
        projection(1,0), // (I_SUBJECT, I_PUB_DATE, I_TITLE),(I_ID)
        IndexScan(itemSubjectDateTitleIndex, firstPara, 50, true)
      )
    )
    def searchBySubjectWI(subject : String) = exec(searchBySubjectPlan, subject)

    val orderDisplayCustomerPlan =
      Selection(
        Equality(projection(0,0),ParameterValue(1)),
        IndexLookup(customer, Array(ParameterValue(0)))
      )
    def orderDisplayCustomerWI(cName : String, cPassword : String) = exec(orderDisplayCustomerPlan, cname, cPassword)

    val orderDisplayOrder =
      IndexLookupJoin(

        IndexScan(
          customerOrderIndex,
          firstPara,
          1,
          false
        )
      )

    def exec(plan: QueryPlan, args: Any*) = {
      val iterator = executor(plan, args:_*)
      iterator.open
      iterator.toList
    }

  //AttributeValue does it start counting from 0 or 1???
  //Questions: Why das Attribute value take a record ID?
  //should we rename KeyGenerator to projection?
}