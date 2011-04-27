package edu.berkeley.cs
package scads
package piql
package modeling

import storage._
import piql.tpcw._

import ch.ethz.systems.tpcw.populate.data._

import scala.util.Random
import net.lag.logging.Logger

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

    QuerySpec(tpcwClient.homeWI, randomCustomer :: Nil) ::
    QuerySpec(tpcwClient.newProductWI, RandomCategory :: perPage :: Nil) ::
    QuerySpec(tpcwClient.productDetailWI, randomItem :: Nil) ::
    QuerySpec(tpcwClient.searchByAuthorWI, randomAuthorName :: perPage :: Nil) ::
    QuerySpec(tpcwClient.searchByTitleWI, randomTitle :: perPage :: Nil) ::
    QuerySpec(tpcwClient.orderDisplayGetCustomer, randomCustomer :: Nil) ::
    QuerySpec(tpcwClient.orderDisplayGetLastOrder, randomCustomer :: Nil) ::
    QuerySpec(tpcwClient.orderDisplayGetOrderLines, randomOrder :: perPage :: Nil) :: Nil toIndexedSeq
  }
}