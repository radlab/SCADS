package edu.berkeley.cs
package scads
package piql
package modeling

import comm._
import storage._
import perf._
import deploylib.ec2._
import deploylib.mesos._
import piql.scadr._
import perf.scadr._
import avro.marker._
import avro.runtime._
import ExperimentUtil._
import scala.collection.mutable.ArrayBuffer

object TpcwModeling {
  object Util {
  	def convolve(hist1: ArrayBuffer[BigInt], hist2: ArrayBuffer[BigInt]): ArrayBuffer[BigInt] = {
  	  var defaultBucketCount = 1000

  	  val result = ArrayBuffer.fill(defaultBucketCount)(BigInt(0))

  	  (0 to defaultBucketCount-1).foreach(i => {
  	    var integralOfOverlap = BigInt(0)

        (0 to i).foreach(j => {
          integralOfOverlap += hist1(i-j) * hist2(j)
        })

        result(i) = integralOfOverlap
  	  })

  	  result
  	}
  	
  	// default bucket size is 1
  	val bucketSize = 1
  	def quantile(hist: ArrayBuffer[BigInt], quantile: Double): Int = {
  	  val quantileAsInt = (quantile * 100).toInt  // convert to percentage; still receive input as percentage to maintain consistency with Histogram's version
      val cumulativeSum = hist.scanLeft(BigInt(0))(_ + _).drop(1)
      if(hist.sum > 0)
        cumulativeSum.findIndexOf(_ >= hist.sum * BigInt(quantileAsInt)/BigInt(100)) * bucketSize
      else
        0
    }
    
    def printActualAndPredicted(actual:Double, predicted:Double) = println(List("actual=", actual, "predicted=", predicted).mkString(", "))
  
    /*
    def average(hist: ArrayBuffer[BigInt]) = {
      val n = hist.sum
      hist.zipWithIndex.foldLeft(BigInt(0)) { case (acc, (num, idx)) => acc + num * idx * bucketSize / n }
    }

    /**
     * Sample standard deviation based off of sqrt(1/(N-1)*sum((Xi-mean(X))^2))
     */
    def stddev(hist: ArrayBuffer[BigInt]) = {
      val n = hist.sum
      val xbar = average(hist)
      import scala.math._
      val a = hist.zipWithIndex.foldLeft(BigInt(0)) { case (acc, (num, idx)) => acc + num * (BigInt(idx) * bucketSize - xbar) * (BigInt(idx) * bucketSize - xbar) }
      sqrt(BigInt(1) / (n - BigInt(1)) * a.toDouble)  // causes compile error
    }
    */
  
  }

  object TpcwData {
    //val clusterAddress = "zk://zoo.knowsql.org/home/kcurtis/scads/experimentCluster0000000008"
    //val clusterAddress = "zk://ec2-75-101-230-218.compute-1.amazonaws.com:2181,ec2-50-19-23-28.compute-1.amazonaws.com:2181,ec2-67-202-16-139.compute-1.amazonaws.com:2181/scads/experimentCluster0000000000"
    //val clusterAddress = "zk://ec2-50-16-25-212.compute-1.amazonaws.com:2181,ec2-67-202-10-38.compute-1.amazonaws.com:2181,ec2-174-129-128-67.compute-1.amazonaws.com:2181/scads/experimentCluster0000000000"
    //val clusterAddress = "zk://ec2-50-19-140-43.compute-1.amazonaws.com:2181,ec2-72-44-35-240.compute-1.amazonaws.com:2181,ec2-50-17-68-210.compute-1.amazonaws.com:2181/scads/experimentCluster0000000000"
    val clusterAddress = "zk://ec2-174-129-157-147.compute-1.amazonaws.com:2181,ec2-50-17-12-53.compute-1.amazonaws.com:2181,ec2-184-72-171-124.compute-1.amazonaws.com:2181/scads/experimentCluster0000000000"

    def experimentResults = allResults.filter(_.clientConfig.clusterAddress == clusterAddress)
    
    def goodExperimentResults = experimentResults.filter(res => res.iteration > 1 && res.iteration <= 50)
        
    val histogramsTpcw = queryTypeHistogram(goodExperimentResults.toSeq)
    
    val perIterationHistograms = queryTypePerIterationHistograms(goodExperimentResults.toSeq)
  }
  
  object ModelBuyRequestExistingCustomerWI {
    import TpcwData._
    import Util._
    
    val buyRequestExistingCustomerWI = QueryDescription("buyRequestExistingCustomerWI", List(), 1)
    val buyRequestExistingCustomerWIHist = histogramsTpcw(buyRequestExistingCustomerWI)
    
    //scala> testTpcwClient.buyRequestExistingCustomerWI.physicalPlan
    //res4: edu.berkeley.cs.scads.piql.QueryPlan = IndexLookupJoin(<Namespace: countries>,ArrayBuffer(AttributeValue(1,6)),
    //                                             IndexLookupJoin(<Namespace: addresses>,ArrayBuffer(AttributeValue(0,4)),
    //                                             IndexLookup(<Namespace: customers>,ArrayBuffer(ParameterValue(0)))))
    val indexLookupCustomers = QueryDescription("indexLookupCustomers", List(), 1)
    val indexLookupCustomersHist = histogramsTpcw(indexLookupCustomers).buckets.map(BigInt(_))

    val indexLookupJoinAddresses = QueryDescription("indexLookupJoinAddresses", List(), 1)
    val indexLookupJoinAddressesHist = histogramsTpcw(indexLookupJoinAddresses).buckets.map(BigInt(_))
    
    val indexLookupJoinCountries = QueryDescription("indexLookupJoinCountries", List(), 1)
    val indexLookupJoinCountriesHist = histogramsTpcw(indexLookupJoinCountries).buckets.map(BigInt(_))
    
    def predictHist = {
      val res1 = convolve(indexLookupCustomersHist, indexLookupJoinAddressesHist)
      val res2 = convolve(res1, indexLookupJoinCountriesHist)
      res2
    }
    
    val actual99th = buyRequestExistingCustomerWIHist.quantile(0.99)
    val predicted99th = quantile(predictHist, 0.99)
    
    def predictOneInterval(i: Int, desiredQuantile: Double): Int = {
      val indexLookupCustomersHist = perIterationHistograms((indexLookupCustomers, i)).buckets.map(BigInt(_))
      val indexLookupJoinAddressesHist = perIterationHistograms((indexLookupJoinAddresses, i)).buckets.map(BigInt(_))
      val indexLookupJoinCountriesHist = perIterationHistograms((indexLookupJoinCountries, i)).buckets.map(BigInt(_))
      
      val res1 = convolve(indexLookupCustomersHist, indexLookupJoinAddressesHist)
      val res2 = convolve(res1, indexLookupJoinCountriesHist)
      res2

      quantile(res2, desiredQuantile)
    }
    
    def getPerIntervalPrediction(quantile: Double = 0.99):(Histogram, Histogram) = {
      val numIntervals = 30
      
      val actualQuantileHist = Histogram(1,1000)
      val predictedQuantileHist = Histogram(1,1000)
      
      println("interval, actualQuantile, predictedQuantile")
      
      (2 to numIntervals).foreach(i => {
        val actualHist = perIterationHistograms((buyRequestExistingCustomerWI, i))
        val actualQuantile = actualHist.quantile(quantile)
        actualQuantileHist += actualQuantile
        
        val predictedQuantile = predictOneInterval(i, quantile)
        predictedQuantileHist += predictedQuantile
        
        println(List(i, actualQuantile, predictedQuantile).mkString(","))
      })
      
      (actualQuantileHist, predictedQuantileHist)
    }
  }
  
  object ModelHomeWI {
    import TpcwData._
    import Util._
    
    val homeWI = QueryDescription("homeWI", List(), 1)
    val homeWIHist = histogramsTpcw(homeWI)
    
    //scala> testTpcwClient.homeWI.physicalPlan
    //res3: edu.berkeley.cs.scads.piql.QueryPlan = IndexLookup(<Namespace: customers>,ArrayBuffer(ParameterValue(0)))
    
    val indexLookupCustomers = QueryDescription("indexLookupCustomers", List(), 1)
    val indexLookupCustomersHist = histogramsTpcw(indexLookupCustomers).buckets.map(BigInt(_))
    
    val actual99th = homeWIHist.quantile(0.99)
    val predicted99th = quantile(indexLookupCustomersHist, 0.99)
    
    def predictOneInterval(i: Int, desiredQuantile: Double): Int = {
      val indexLookupCustomersHist = perIterationHistograms((indexLookupCustomers, i)).buckets.map(BigInt(_))
      
      quantile(indexLookupCustomersHist, desiredQuantile)
    }
    
    def getPerIntervalPrediction(quantile: Double = 0.99):(Histogram, Histogram) = {
      val numIntervals = 30
      
      val actualQuantileHist = Histogram(1,1000)
      val predictedQuantileHist = Histogram(1,1000)
      
      println("interval, actualQuantile, predictedQuantile")
      
      (2 to numIntervals).foreach(i => {
        val actualHist = perIterationHistograms((homeWI, i))
        val actualQuantile = actualHist.quantile(quantile)
        actualQuantileHist += actualQuantile
        
        val predictedQuantile = predictOneInterval(i, quantile)
        predictedQuantileHist += predictedQuantile
        
        println(List(i, actualQuantile, predictedQuantile).mkString(","))
      })
      
      (actualQuantileHist, predictedQuantileHist)
    }
    
  }
  
  // yikes, doesn't work...
  // maybe not enough samples?
  object ModelNewProductWI {
    import TpcwData._
    import Util._
    
    val newProductWI = QueryDescription("newProductWI", List(50), 50)
    val newProductWIHist = histogramsTpcw(newProductWI)
    
    //scala> testTpcwClient.newProductWI.physicalPlan
    //res6: edu.berkeley.cs.scads.piql.QueryPlan = IndexLookupJoin(<Namespace: authors>,List(AttributeValue(1,2)),
    //                                             IndexLookupJoin(<Namespace: items>,List(AttributeValue(0,2)),LocalStopAfter(ParameterLimit(1,50),
    //                                             IndexScan(<Namespace: items_({"fieldNames": ["I_SUBJECT"]},{"fieldName": "I_PUB_DATE"})>,List(ParameterValue(0)),ParameterLimit(1,50),false))))
    
    
    val indexScanItemsIdx = QueryDescription("indexScanItemsIdx", List(50), 50)
    val indexScanItemsIdxHist = histogramsTpcw(indexScanItemsIdx).buckets.map(BigInt(_))

    val indexLookupJoinItems = QueryDescription("indexLookupJoinItems", List(50), 50)
    val indexLookupJoinItemsHist = histogramsTpcw(indexLookupJoinItems).buckets.map(BigInt(_))

    val indexLookupJoinAuthors = QueryDescription("indexLookupJoinAuthors", List(50), 50)
    val indexLookupJoinAuthorsHist = histogramsTpcw(indexLookupJoinAuthors).buckets.map(BigInt(_))
    
    def predictHist = {
      val res1 = convolve(indexScanItemsIdxHist, indexLookupJoinItemsHist)
      val res2 = convolve(res1, indexLookupJoinAuthorsHist)
      res2
    }
    
    val actual99th = newProductWIHist.quantile(0.99)
    val predicted99th = quantile(predictHist, 0.99)
    
    def predictOneInterval(i: Int, desiredQuantile: Double): Int = {
      val indexScanItemsIdxHist = perIterationHistograms((indexScanItemsIdx, i)).buckets.map(BigInt(_))
      val indexLookupJoinItemsHist = perIterationHistograms((indexLookupJoinItems, i)).buckets.map(BigInt(_))
      val indexLookupJoinAuthorsHist = perIterationHistograms((indexLookupJoinAuthors, i)).buckets.map(BigInt(_))
      
      val res1 = convolve(indexScanItemsIdxHist, indexLookupJoinItemsHist)
      val res2 = convolve(res1, indexLookupJoinAuthorsHist)
      res2

      quantile(res2, desiredQuantile)
    }
    
    def getPerIntervalPrediction(quantile: Double = 0.99):(Histogram, Histogram) = {
      val numIntervals = 30
      
      val actualQuantileHist = Histogram(1,1000)
      val predictedQuantileHist = Histogram(1,1000)
      
      println("interval, actualQuantile, predictedQuantile")
      
      (2 to numIntervals).foreach(i => {
        val actualHist = perIterationHistograms((newProductWI, i))
        val actualQuantile = actualHist.quantile(quantile)
        actualQuantileHist += actualQuantile
        
        val predictedQuantile = predictOneInterval(i, quantile)
        predictedQuantileHist += predictedQuantile
        
        println(List(i, actualQuantile, predictedQuantile).mkString(","))
      })
      
      (actualQuantileHist, predictedQuantileHist)
    }
    
  }
  
  object ModelOrderDisplayGetCustomer {
    import TpcwData._
    import Util._
    
    val orderDisplayGetCustomer = QueryDescription("orderDisplayGetCustomer", List(), 1)
    val orderDisplayGetCustomerHist = histogramsTpcw(orderDisplayGetCustomer)
    
    //scala> testTpcwClient.orderDisplayGetCustomer.physicalPlan
    //res11: edu.berkeley.cs.scads.piql.QueryPlan = IndexLookup(<Namespace: customers>,ArrayBuffer(ParameterValue(0)))
    
    val indexLookupCustomers = QueryDescription("indexLookupCustomers", List(), 1)
    val indexLookupCustomersHist = histogramsTpcw(indexLookupCustomers)
    
    val actual99th = orderDisplayGetCustomerHist.quantile(0.99)
    val predicted99th = indexLookupCustomersHist.quantile(0.99)
  }
  
  object ModelOrderDisplayGetLastOrder {
    import TpcwData._
    
    val orderDisplayGetLastOrder = QueryDescription("orderDisplayGetLastOrder", List(), 1)  // queryName, params, resultCardinality
    val orderDisplayGetLastOrderHist = histogramsTpcw(orderDisplayGetLastOrder)
    
    //scala> res3.orderDisplayGetLastOrder.physicalPlan
    //res4: edu.berkeley.cs.scads.piql.QueryPlan = IndexLookupJoin(<Namespace: countries>,ArrayBuffer(AttributeValue(2,6)),
    //                                             IndexLookupJoin(<Namespace: addresses>,ArrayBuffer(AttributeValue(1,9)),
    //                                             IndexLookupJoin(<Namespace: countries>,ArrayBuffer(AttributeValue(2,6)),
    //                                             IndexLookupJoin(<Namespace: addresses>,ArrayBuffer(AttributeValue(1,8)),LocalStopAfter(FixedLimit(1),
    //                                             IndexLookupJoin(<Namespace: orders>,ArrayBuffer(AttributeValue(0,2)),
    //                                             IndexScan(<Namespace: orders_({"fieldName": "O_C_UNAME"},{"fieldName": "O_DATE_Time"})>,ArrayBuffer(ParameterValue(0)),FixedLimit(1),false)))))))
    val indexScanOrdersIdx = QueryDescription("indexScanOrdersIdx", List(), 1)
    val indexScanOrdersIdxHist = histogramsTpcw(indexScanOrdersIdx).buckets.map(BigInt(_))
    
    val indexLookupJoinOrders = QueryDescription("indexLookupJoinOrders", List(), 1)
    val indexLookupJoinOrdersHist = histogramsTpcw(indexLookupJoinOrders).buckets.map(BigInt(_))
    
    val indexLookupJoinAddresses = QueryDescription("indexLookupJoinAddresses", List(), 1)
    val indexLookupJoinAddressesHist = histogramsTpcw(indexLookupJoinAddresses).buckets.map(BigInt(_))
    
    val indexLookupJoinCountries = QueryDescription("indexLookupJoinCountries", List(), 1)
    val indexLookupJoinCountriesHist = histogramsTpcw(indexLookupJoinCountries).buckets.map(BigInt(_))
    
    //val predictedHist = indexLookupJoinCountriesHist convolveWith indexLookupJoinAddressesHist convolveWith indexLookupJoinCountriesHist convolveWith indexLookupJoinAddressesHist convolveWith indexLookupJoinOrdersHist convolveWith indexScanOrdersIdxHist
    // results in overflow.  instead, try per-interval prediction
            
    import Util._

    def predictHist = {
      val res1 = convolve(indexScanOrdersIdxHist, indexLookupJoinOrdersHist)
      val res2 = convolve(res1, indexLookupJoinAddressesHist)
      val res3 = convolve(res2, indexLookupJoinCountriesHist)
      val res4 = convolve(res3, indexLookupJoinAddressesHist)
      val res5 = convolve(res4, indexLookupJoinCountriesHist)
      res5
    }
    
    def predictOneInterval(i: Int, desiredQuantile: Double): Int = {
      val indexScanOrdersIdxHist = perIterationHistograms((indexScanOrdersIdx, i)).buckets.map(BigInt(_))
      val indexLookupJoinOrdersHist = perIterationHistograms((indexLookupJoinOrders, i)).buckets.map(BigInt(_))
      val indexLookupJoinAddressesHist = perIterationHistograms((indexLookupJoinAddresses, i)).buckets.map(BigInt(_))
      val indexLookupJoinCountriesHist = perIterationHistograms((indexLookupJoinCountries, i)).buckets.map(BigInt(_))
      
      val res1 = convolve(indexScanOrdersIdxHist, indexLookupJoinOrdersHist)
      val res2 = convolve(res1, indexLookupJoinAddressesHist)
      val res3 = convolve(res2, indexLookupJoinCountriesHist)
      val res4 = convolve(res3, indexLookupJoinAddressesHist)
      val predictedHist = convolve(res4, indexLookupJoinCountriesHist)

      quantile(predictedHist, desiredQuantile)
    }
        
    def getPerIntervalPrediction(quantile: Double = 0.99):(Histogram, Histogram) = {
      val numIntervals = 30

      val actualQuantileHist = Histogram(1,1000)
      val predictedQuantileHist = Histogram(1,1000)

      println("interval, actualQuantile, predictedQuantile")

      (2 to numIntervals).foreach(i => {
        val actualHist = perIterationHistograms((orderDisplayGetLastOrder, i))
        val actualQuantile = actualHist.quantile(quantile)
        actualQuantileHist += actualQuantile

        val predictedQuantile = predictOneInterval(i, quantile)
        predictedQuantileHist += predictedQuantile
        
        println(List(i, actualQuantile, predictedQuantile).mkString(","))
      })

      (actualQuantileHist, predictedQuantileHist)
    }
    
    val actual99th = orderDisplayGetLastOrderHist.quantile(0.99)
    val predicted99th = quantile(predictHist, 0.99)
  }
  
  object ModelOrderDisplayGetOrderLines {
    import TpcwData._
    import Util._
    
    val orderDisplayGetOrderLines = QueryDescription("orderDisplayGetOrderLines", List(10), 3)  // might want to fix this later so requested cardinality = result cardinality
    val orderDisplayGetOrderLinesHist = histogramsTpcw(orderDisplayGetOrderLines)
    
    //scala> testTpcwClient.orderDisplayGetOrderLines.physicalPlan
    //res13: edu.berkeley.cs.scads.piql.QueryPlan = IndexLookupJoin(<Namespace: items>,ArrayBuffer(AttributeValue(0,2)),LocalStopAfter(ParameterLimit(1,100),
    //                                              IndexScan(<Namespace: orderLines>,ArrayBuffer(ParameterValue(0)),ParameterLimit(1,100),true)))
    
    val indexScanOrderLines = QueryDescription("indexScanOrderLines", List(10), 3)
    val indexScanOrderLinesHist = histogramsTpcw(indexScanOrderLines).buckets.map(BigInt(_))
    
    val indexLookupJoinItems = QueryDescription("indexLookupJoinItems", List(3), 3)
    val indexLookupJoinItemsHist = histogramsTpcw(indexLookupJoinItems).buckets.map(BigInt(_))
    
    def predictHist = {
      convolve(indexScanOrderLinesHist, indexLookupJoinItemsHist)
    }
    
    val actual99th = orderDisplayGetOrderLinesHist.quantile(0.99)
    val predicted99th = quantile(predictHist, 0.99)
    
    def predictOneInterval(i: Int, desiredQuantile: Double): Int = {
      val indexScanOrderLinesHist = perIterationHistograms((indexScanOrderLines, i)).buckets.map(BigInt(_))
      val indexLookupJoinItemsHist = perIterationHistograms((indexLookupJoinItems, i)).buckets.map(BigInt(_))
      
      val predictedHist = convolve(indexScanOrderLinesHist, indexLookupJoinItemsHist)
      
      quantile(predictedHist, desiredQuantile)
    }
    
    def getPerIntervalPrediction(quantile: Double = 0.99):(Histogram, Histogram) = {
      val numIntervals = 30
      
      val actualQuantileHist = Histogram(1,1000)
      val predictedQuantileHist = Histogram(1,1000)
      
      println("interval, actualQuantile, predictedQuantile")
      
      (2 to numIntervals).foreach(i => {
        val actualHist = perIterationHistograms((orderDisplayGetOrderLines, i))
        val actualQuantile = actualHist.quantile(quantile)
        actualQuantileHist += actualQuantile
        
        val predictedQuantile = predictOneInterval(i, quantile)
        predictedQuantileHist += predictedQuantile
        
        println(List(i, actualQuantile, predictedQuantile).mkString(","))
      })
      
      (actualQuantileHist, predictedQuantileHist)
    }
  }

  object ModelProductDetailWI {
    import TpcwData._
    import Util._
    
    val productDetailWI = QueryDescription("productDetailWI", List(), 1)
    val productDetailWIHist = histogramsTpcw(productDetailWI)
    
    //scala> testTpcwClient.productDetailWI.physicalPlan
    //res17: edu.berkeley.cs.scads.piql.QueryPlan = IndexLookupJoin(<Namespace: authors>,ArrayBuffer(AttributeValue(0,2)),
    //                                              IndexLookup(<Namespace: items>,ArrayBuffer(ParameterValue(0))))
    
    val indexLookupItems = QueryDescription("indexLookupItems", List(), 1)
    val indexLookupItemsHist = histogramsTpcw(indexLookupItems).buckets.map(BigInt(_))
    
    val indexLookupJoinAuthors = QueryDescription("indexLookupJoinAuthors", List(1), 1)
    val indexLookupJoinAuthorsHist = histogramsTpcw(indexLookupJoinAuthors).buckets.map(BigInt(_))
    
    def predictHist = {
      convolve(indexLookupItemsHist, indexLookupJoinAuthorsHist)
    }
    
    val actual99th = productDetailWIHist.quantile(0.99)
    val predicted99th = quantile(predictHist, 0.99)
    
    def predictOneInterval(i: Int, desiredQuantile: Double): Int = {
      val indexLookupItemsHist = perIterationHistograms((indexLookupItems, i)).buckets.map(BigInt(_))
      val indexLookupJoinAuthorsHist = perIterationHistograms((indexLookupJoinAuthors, i)).buckets.map(BigInt(_))

      quantile(convolve(indexLookupItemsHist, indexLookupJoinAuthorsHist), desiredQuantile)
    }
        
    def getPerIntervalPrediction(quantile: Double = 0.99):(Histogram, Histogram) = {
      val numIntervals = 30

      val actualQuantileHist = Histogram(1,1000)
      val predictedQuantileHist = Histogram(1,1000)

      println("interval, actualQuantile, predictedQuantile")

      (2 to numIntervals).foreach(i => {
        val actualHist = perIterationHistograms((productDetailWI, i))
        val actualQuantile = actualHist.quantile(quantile)
        actualQuantileHist += actualQuantile

        val predictedQuantile = predictOneInterval(i, quantile)
        predictedQuantileHist += predictedQuantile
        
        println(List(i, actualQuantile, predictedQuantile).mkString(","))
      })

      (actualQuantileHist, predictedQuantileHist)
    }
    
  }

  // for some reason, I don't have data on this
  // will have to figure this out later
  // probably b/c in TpcwClient, there was no query name passed to toPiql
  // right now, only have a query description where shopping cart is empty b/c shopping cart is never getting populated
  // for now, postpone this
  object ModelRetrieveShoppingCart {
    import TpcwData._
    import Util._
    
    val retrieveShoppingCart = QueryDescription("unnamed", List(), 0) // "unnamed" b/c of bug in TpcwClient; 0 b/c not getting populated
    val retrieveShoppingCartHist = histogramsTpcw(retrieveShoppingCart)
    
    //scala> testTpcwClient.retrieveShoppingCart.physicalPlan
    //res33: edu.berkeley.cs.scads.piql.QueryPlan = IndexLookupJoin(<Namespace: items>,ArrayBuffer(AttributeValue(0,1)),
    //                                              LocalStopAfter(FixedLimit(1000),
    //                                              IndexScan(<Namespace: shoppingCartItems>,ArrayBuffer(ParameterValue(0)),FixedLimit(1000),true)))
    
    
  }

  object ModelSearchByAuthorWI {
    import TpcwData._
    import Util._
    
    val searchByAuthorWI = QueryDescription("searchByAuthorWI", List(10), 10) // only one where requested cardinality = result cardinality
    val searchByAuthorWIHist = histogramsTpcw(searchByAuthorWI)
    
    //scala> testTpcwClient.searchByAuthorWI.physicalPlan
    //res22: edu.berkeley.cs.scads.piql.QueryPlan = LocalStopAfter(ParameterLimit(1,50),
    //                                              IndexMergeJoin(<Namespace: items_({"fieldName": "I_A_ID"},{"fieldName": "I_TITLE"})>,List(AttributeValue(0,1)),List(AttributeValue(2,1)),ParameterLimit(1,50),true,
    //                                              IndexLookupJoin(<Namespace: authors>,List(AttributeValue(0,1)),LocalStopAfter(FixedLimit(50),
    //                                              IndexScan(<Namespace: authors_({"fieldNames": ["A_FNAME", "A_LNAME"]})>,List(ParameterValue(0)),FixedLimit(50),true)))))
    
    // I don't think we have the right benchmarks collected here.
    // I think we need the following:
    
    val indexScanAuthorsIdx = QueryDescription("indexScanAuthorsIdx", List(10), 1)  // i have this benchmark already
    
    val indexLookupJoinAuthors = QueryDescription("indexLookupJoinAuthors", List(1), 1) // i NEED this
    
    val indexMergeJoinItemsIdx = QueryDescription("indexMergeJoinItemsIdx", List(1, 10), 10) // i NEED this
  }
  
  object ModelSearchByTitleWI {
    import TpcwData._
    import Util._
    
    val searchByTitleWI = QueryDescription("searchByTitleWI", List(10), 9)  // want to make requested, result cardinalities match
    val searchByTitleWIHist = histogramsTpcw(searchByTitleWI)
    
    //scala> testTpcwClient.searchByTitleWI.physicalPlan
    //res26: edu.berkeley.cs.scads.piql.QueryPlan = IndexLookupJoin(<Namespace: authors>,List(AttributeValue(0,2)),LocalStopAfter(ParameterLimit(1,50),
    //                                              IndexScan(<Namespace: items_({"fieldNames": ["I_TITLE"]},{"fieldName": "I_TITLE"},{"fieldName": "I_A_ID"})>,List(ParameterValue(0)),ParameterLimit(1,50),true)))
    
    val indexScanItemsIdx = QueryDescription("indexScanItemsIdx", List(9), 9)
    val indexScanItemsIdxHist = histogramsTpcw(indexScanItemsIdx).buckets.map(BigInt(_))
    
    val indexLookupJoinAuthors = QueryDescription("indexLookupJoinAuthors", List(9), 9)
    val indexLookupJoinAuthorsHist = histogramsTpcw(indexLookupJoinAuthors).buckets.map(BigInt(_))
    
    def predictHist = {
      convolve(indexScanItemsIdxHist, indexLookupJoinAuthorsHist)
    }
    
    val actual99th = searchByTitleWIHist.quantile(0.99)
    val predicted99th = quantile(predictHist, 0.99)
    
    def predictOneInterval(i: Int, desiredQuantile: Double): Int = {
      val indexScanItemsIdxHist = perIterationHistograms((indexScanItemsIdx, 1)).buckets.map(BigInt(_))
      val indexLookupJoinAuthorsHist = perIterationHistograms((indexLookupJoinAuthors, 1)).buckets.map(BigInt(_))

       quantile(convolve(indexScanItemsIdxHist, indexLookupJoinAuthorsHist), desiredQuantile)
     }

     def getPerIntervalPrediction(quantile: Double = 0.99):(Histogram, Histogram) = {
       val numIntervals = 30

       val actualQuantileHist = Histogram(1,1000)
       val predictedQuantileHist = Histogram(1,1000)

       println("interval, actualQuantile, predictedQuantile")

       (2 to numIntervals).foreach(i => {
         val actualHist = perIterationHistograms((searchByTitleWI, i))
         val actualQuantile = actualHist.quantile(quantile)
         actualQuantileHist += actualQuantile

         val predictedQuantile = predictOneInterval(i, quantile)
         predictedQuantileHist += predictedQuantile

         println(List(i, actualQuantile, predictedQuantile).mkString(","))
       })

       (actualQuantileHist, predictedQuantileHist)
     }
    
  }
}