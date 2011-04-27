package edu.berkeley.cs
package scads
package piql
package modeling

import storage._
import piql.tpcw._

import ch.ethz.systems.tpcw.populate.data._

import scala.util.Random
import net.lag.logging.Logger
import collection.mutable.ArrayBuffer
import comm._

class RandomC_UNAME(tpcwData: TpcwLoader) extends ParameterGenerator {
  final def getValue(rand: Random) = {
    (tpcwData.toCustomer(rand.nextInt(tpcwData.numCustomers) + 1), None)
  }
}

object RandomCategory extends ParameterGenerator {
  import ch.ethz.systems.tpcw.populate.data._

  final def getValue(rand: Random) = {
    (Utils.SUBJECTS(rand.nextInt(Utils.SUBJECTS.size)), None)
  }
}

class RandomTitle(tpcwData: TpcwLoader) extends ParameterGenerator {
  final def getValue(rand: Random) = {
    (tpcwData.createItem(rand.nextInt(tpcwData.numItems) + 1).I_TITLE.split(" ").filterNot(_.size == 0).head, None)
  }
}

class RandomItem(tpcwData: TpcwLoader) extends ParameterGenerator {
  final def getValue(rand: Random) = {
    (tpcwData.toItem(rand.nextInt(tpcwData.numItems) + 1), None)
  }
}

class RandomAuthorName(tpcwData: TpcwLoader) extends ParameterGenerator {
  final def getValue(rand: Random) = {
    (tpcwData.toAuthorFname(tpcwData.toAuthor(rand.nextInt(tpcwData.numAuthors) + 1)), None)
  }
}

class RandomOrder(tpcwData: TpcwLoader) extends ParameterGenerator {
  final def getValue(rand: Random) = {
    (tpcwData.toOrder(rand.nextInt(tpcwData.numOrders) + 1), None)
  }
}

class RandomItemList(tpcwData: TpcwLoader, cardinalities: IndexedSeq[Int]) extends ParameterGenerator {
  final def getValue(rand: Random) = {
    val cardinality = cardinalities(rand.nextInt(cardinalities.size))
    (ArrayBuffer.fill(cardinality)(ArrayBuffer(StringRec(tpcwData.toItem(rand.nextInt(tpcwData.numItems) + 1)))), Some(cardinality))
  }
}

class RandomAuthorList(tpcwData: TpcwLoader, cardinalities: IndexedSeq[Int]) extends ParameterGenerator {
  final def getValue(rand: Random) = {
    val cardinality = cardinalities(rand.nextInt(cardinalities.size))
    (ArrayBuffer.fill(cardinality)(ArrayBuffer(StringRec(tpcwData.toAuthor(rand.nextInt(tpcwData.numAuthors) + 1)))), Some(cardinality))
  }
}

// returns a single country ID, but must be in a list since it'll be used by LocalIterator
class RandomCountryIdList(tpcwData: TpcwLoader) extends ParameterGenerator {
  import ch.ethz.systems.tpcw.populate.data._

  final def getValue(rand: Random) = {
    (ArrayBuffer(ArrayBuffer(IntRec(rand.nextInt(Utils.COUNTRIES.size) + 1))), None)
  }
}

// returns a single address, but must be in a list since it'll be used by LocalIterator
class RandomAddressList(tpcwData: TpcwLoader) extends ParameterGenerator {
  final def getValue(rand: Random) = {
    (ArrayBuffer(ArrayBuffer(StringRec(tpcwData.toAddress(rand.nextInt(tpcwData.numAddresses) + 1)))), None)
  }
}

class TpcwQueryProvider extends QueryProvider {
  val logger = Logger()

  def getQueryList(cluster: ScadsCluster, executor: QueryExecutor): IndexedSeq[QuerySpec] = {
    val clusterConfig = cluster.root.awaitChild("clusterReady").data
    val loaderConfig = classOf[TpcwLoaderTask].newInstance.parse(clusterConfig)
    val tpcwClient = new TpcwClient(cluster, executor)
    val tpcwData = new TpcwLoader(loaderConfig.numEBs, loaderConfig.numItems)

    val randomCustomer = new RandomC_UNAME(tpcwData)
    val randomTitle = new RandomTitle(tpcwData)
    val randomItem = new RandomItem(tpcwData)
    val randomAuthorName = new RandomAuthorName(tpcwData)
    val randomOrder = new RandomOrder(tpcwData)
    val perPage = CardinalityList(10 to 100 by 10 toIndexedSeq)
    val randomItemList = new RandomItemList(tpcwData, perPage.values)


    // iterator definitions
    val indexLookupCustomers = new OptimizedQuery(
      "indexLookupCustomers",
      IndexLookup(
        tpcwClient.customers,
        AttributeValue(0,0) :: Nil
      ),
      executor
    )
    
    val indexLookupItems = new OptimizedQuery(
      "indexLookupItems",
      IndexLookup(
        tpcwClient.items,
        AttributeValue(0,0) :: Nil
      ),
      executor
    )

    val indexScanItemsIdx = new OptimizedQuery(
      "indexScanItemsIdx", 
      IndexScan(
        tpcwClient.items.getOrCreateIndex(TokenIndex("I_SUBJECT" :: Nil) :: AttributeIndex("I_PUB_DATE") :: Nil),
        (0.?) :: Nil,
        ParameterLimit(1, 500),
        false
      ),
      executor
    )

    val indexScanOrdersIdx = new OptimizedQuery(
      "indexScanOrdersIdx",
      IndexScan(
        tpcwClient.orders.getOrCreateIndex(AttributeIndex("O_C_UNAME") :: AttributeIndex("O_DATE_Time") :: Nil),
        (0.?) :: Nil,
        ParameterLimit(1,500),
        false
      ),
      executor
    )

    val indexScanOrdersIdxSingleItem = new OptimizedQuery(
      "indexScanOrdersIdxSingleItem", 
      IndexScan(
        tpcwClient.orders.getOrCreateIndex(AttributeIndex("O_C_UNAME") :: AttributeIndex("O_DATE_Time") :: Nil),
        (0.?) :: Nil,
        FixedLimit(1),
        false
      ),
      executor
    )
    
    val indexScanAuthorsIdx = new OptimizedQuery(
      "indexScanAuthorsIdx",
      IndexScan(
        tpcwClient.authors.getOrCreateIndex(TokenIndex("A_FNAME" :: "A_LNAME" :: Nil) :: Nil),
        (0.?) :: Nil,
        ParameterLimit(1, 50),
        true
      ),
      executor
    )
    
    val indexScanOrderLines = new OptimizedQuery(
      "indexScanOrderLines",
      IndexScan(
        tpcwClient.orderLines,
        (0.?) :: Nil,
        ParameterLimit(1, 50),
        false // don't know if this is right
      ),
      executor
    )
    
    val indexLookupJoinItems = new OptimizedQuery(
      "indexLookupJoinItems",
      IndexLookupJoin(
        tpcwClient.items,
        AttributeValue(0,0) :: Nil,  // first record, first field
        LocalIterator(0)             // which param in query are you passing this to
      ),
      executor
    )
    
    val indexLookupJoinAuthors = new OptimizedQuery(
      "indexLookupJoinAuthors",
      IndexLookupJoin(
        tpcwClient.authors,
        AttributeValue(0,0) :: Nil,
        LocalIterator(0)
      ),
      executor
    )
    
    val indexLookupJoinCountries = new OptimizedQuery(
      "indexLookupJoinCountries",
      IndexLookupJoin(
        tpcwClient.countries,
        AttributeValue(0,0) :: Nil,
        LocalIterator(0)
      ),
      executor
    )
    
    val indexLookupJoinAddresses = new OptimizedQuery(
      "indexLookupJoinCountries",
      IndexLookupJoin(
        tpcwClient.addresses,
        AttributeValue(0,0) :: Nil,
        LocalIterator(0)
      ),
      executor
    )

    val indexLookupJoinOrders = new OptimizedQuery(
      "indexLookupJoinOrders",
      IndexLookupJoin(
        tpcwClient.orders,
        AttributeValue(0,0) :: Nil,
        LocalIterator(0)
      ),
      executor
    )
    
    val indexMergeJoinItemsIdx = new OptimizedQuery(
      "indexMergeJoinItemsIdx",
			LocalStopAfter( // should I be putting these into the above queries?
				ParameterLimit(1, 500),
				IndexMergeJoin(
          tpcwClient.items.getOrCreateIndex(TokenIndex("I_SUBJECT" :: Nil) :: AttributeIndex("I_PUB_DATE") :: Nil),
					AttributeValue(0,1) :: Nil, // 1st record, 2nd field -- don't know if this is right
					AttributeValue(1,1) :: Nil, // 2nd record, 2nd field -- don't know if this is right
					ParameterLimit(1, 500),
					false,
					LocalIterator(0))
				),
			executor
		)
    
    // queries
    QuerySpec(tpcwClient.homeWI, randomCustomer :: Nil) ::  // indexLookupCustomers
    QuerySpec(tpcwClient.newProductWI, RandomCategory :: perPage :: Nil) :: // indexLookupJoinAuthors, indexLookupJoinItems, indexScanItemsIdx
    QuerySpec(tpcwClient.productDetailWI, randomItem :: Nil) :: // indexLookupJoinAuthors, indexLookupItems
    QuerySpec(tpcwClient.searchByAuthorWI, randomAuthorName :: perPage :: Nil) :: // indexMergeJoinItemsIdx, indexLookupJoinAuthors, indexScanAuthorsIdx
    QuerySpec(tpcwClient.searchByTitleWI, randomTitle :: perPage :: Nil) :: // indexLookupJoinAuthors, indexScanItemsIdx
    QuerySpec(tpcwClient.orderDisplayGetCustomer, randomCustomer :: Nil) :: // indexLookupCustomers
    QuerySpec(tpcwClient.orderDisplayGetLastOrder, randomCustomer :: Nil) ::  // indexLookupJoinCountries, indexLookupJoinAddresses, (repeat both), indexLookupJoinOrders, indexScanOrdersIdx
    QuerySpec(tpcwClient.orderDisplayGetOrderLines, randomOrder :: perPage :: Nil) :: // indexLookupJoinItems, indexScanOrderLines
    // iterators
    QuerySpec(indexScanItemsIdx, RandomCategory :: perPage :: Nil) ::
    QuerySpec(indexScanOrdersIdxSingleItem, randomCustomer :: Nil) ::
    QuerySpec(indexLookupJoinItems, randomItemList :: Nil) ::
    // add other indexLookupJoin queries too
    Nil toIndexedSeq
  }
}