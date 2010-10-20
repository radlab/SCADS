package edu.berkeley.cs.scads.piql

import edu.berkeley.cs.scads.storage._
import edu.berkeley.cs.avro.marker._

import org.apache.avro.util._
import scala.util.Random

class TpcwClient(val cluster: ScadsCluster, val executor: QueryExecutor) {
  lazy val address = cluster.getNamespace[AddressKey, AddressValue]("address")
  lazy val author = cluster.getNamespace[AuthorKey, AuthorValue]("author")
  lazy val authorNameItemIndex = cluster.getNamespace[AuthorNameItemIndexKey, NullRecord]("author_fname_index")
  //val authorLNameIndex = cluster.getNamespace[AuthorLNameIndexKey, NullRecord]("author_lname_index") //make it one
  lazy val xacts = cluster.getNamespace[CcXactsKey, CcXactsValue]("xacts")
  lazy val country = cluster.getNamespace[CountryKey, CountryValue]("country")
  lazy val customer = cluster.getNamespace[CustomerKey, CustomerValue]("customer")
  lazy val item = cluster.getNamespace[ItemKey, ItemValue]("item")
  lazy val itemSubjectDateTitleIndex = cluster.getNamespace[ItemSubjectDateTitleIndexKey, ItemKey]("item_subject_date_title_index")
  lazy val orderline = cluster.getNamespace[OrderLineKey, OrderLineValue]("orderline")
  lazy val order = cluster.getNamespace[OrdersKey, OrdersValue]("orders")
  lazy val customerOrderIndex = cluster.getNamespace[CustomerOrderIndex, NullRecord]("customer_index")  //Extra index
  lazy val itemTitleIndex = cluster.getNamespace[ItemTitleIndexKey, NullRecord]("item_title_index")

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
        firstPara, //Stephen: Fix it
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
  def searchBySubjectWI(subject : String) = exec(searchBySubjectPlan, new Utf8(subject))


  private lazy val orderDisplayCustomerPlan =
    Selection(
    Equality(AttributeValue(0,0),ParameterValue(1)),
    IndexLookup(customer, paraSelector(0))  // CustomerName, (C_PASSWD, C_FNAME, C_LNAME,....)
    )
  def orderDisplayCustomerWI(cName : String, cPassword : String) = exec(orderDisplayCustomerPlan, new Utf8(cName), new Utf8(cPassword))

//  val orderDisplayOrder =
//    IndexLookupJoin(
//
//    IndexScan(
//      customerOrderIndex,
//      firstPara,
//      1,
//      false
//    )
//    )

  def exec(plan: QueryPlan, args: Any*) = {
    val iterator = executor(plan, args:_*)
    iterator.open
    iterator.toList
  }

  def loadData(numEBs : Double, numItems : Int) = {
    val loader = new TpcwLoader(this, numEBs, numItems)
    loader.load()
  }

}
