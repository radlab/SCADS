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

object ScadrModeling {
  object ScadrData {
    val clusterAddress = "zk://ec2-50-17-12-53.compute-1.amazonaws.com:2181,ec2-184-72-171-124.compute-1.amazonaws.com:2181,ec2-174-129-157-147.compute-1.amazonaws.com:2181/scads/experimentCluster0000000063"
    

    def experimentResults = allResults.filter(_.clientConfig.clusterAddress == clusterAddress)
    
    val numIntervals = 14
    def goodExperimentResults = experimentResults.filter(r => r.iteration > 1 && r.iteration <= numIntervals)
    
    val histogramsScadr = queryTypeHistogram(goodExperimentResults.toSeq)
    
    val perIterationHistograms = queryTypePerIterationHistograms(goodExperimentResults.toSeq)
  }
  
  /*
  object ModelFindSubscription {
    import ScadrData._
    
    val findSubscription = QueryDescription("findSubscription", List(), 1)
    val findSubscriptionHist = histogramsScadr(findSubscription)
    
    //scala> res1.findSubscription.physicalPlan
    //res6: edu.berkeley.cs.scads.piql.QueryPlan = IndexLookup(<Namespace: subscriptions>,ArrayBuffer(ParameterValue(0), ParameterValue(1)))
    
    val indexLookupSubscriptions = QueryDescription("indexLookupSubscriptions", List(), 1)
  }
  */
  
  object ModelFindUser {
    import ScadrData._
    
    val findUser = QueryDescription("findUser", List(), 1)
    val findUserHist = histogramsScadr(findUser)
    
    //scala> res1.findUser.physicalPlan
    //res2: edu.berkeley.cs.scads.piql.QueryPlan = IndexLookup(<Namespace: users>,ArrayBuffer(ParameterValue(0)))
    
    val indexLookupUsers = QueryDescription("indexLookupUsers", List(), 1)
    val indexLookupUsersHist = histogramsScadr(indexLookupUsers)
    
    val actual99th = findUserHist.quantile(0.99)
    val predicted99th = indexLookupUsersHist.quantile(0.99)
   
    def predictOneInterval(i: Int, desiredQuantile: Double): Int = {
      val indexLookupUsersHist = perIterationHistograms((indexLookupUsers, i))
      indexLookupUsersHist.quantile(desiredQuantile)
    }

    def getPerIntervalPrediction(quantile: Double = 0.99):(Histogram, Histogram) = {
      val actualQuantileHist = Histogram(1,1000)
      val predictedQuantileHist = Histogram(1,1000)

      println("interval, actualQuantile, predictedQuantile")

      (2 to numIntervals).foreach(i => {
        val actualHist = perIterationHistograms((findUser, i))
        val actualQuantile = actualHist.quantile(quantile)
        actualQuantileHist += actualQuantile

        val predictedQuantile = predictOneInterval(i, quantile)
        predictedQuantileHist += predictedQuantile

        println(List(i, actualQuantile, predictedQuantile).mkString(","))
      })

      (actualQuantileHist, predictedQuantileHist)
    }
  }
  
  object ModelMyThoughts {
    import ScadrData._
    
    val myThoughts = QueryDescription("myThoughts", List(50), 50) // TODO:  choose cardinality here
    val myThoughtsHist = histogramsScadr(myThoughts)
    
    //scala> res1.myThoughts.physicalPlan
    //res3: edu.berkeley.cs.scads.piql.QueryPlan = LocalStopAfter(ParameterLimit(1,10000),
    //                                             IndexScan(<Namespace: thoughts>,ArrayBuffer(ParameterValue(0)),ParameterLimit(1,10000),false))
    
    val indexScanThoughts = QueryDescription("indexScanThoughts", List(50), 50) // TODO:  make this match cardinality for myThoughts
    val indexScanThoughtsHist = histogramsScadr(myThoughts)
    
    val actual99th = myThoughtsHist.quantile(0.99)
    val predicted99th = indexScanThoughtsHist.quantile(0.99)
    
    def predictOneInterval(i: Int, desiredQuantile: Double): Int = {
      val indexScanThoughtsHist = perIterationHistograms((indexScanThoughts, i))
      indexScanThoughtsHist.quantile(desiredQuantile)
    }

    def getPerIntervalPrediction(quantile: Double = 0.99):(Histogram, Histogram) = {
      val actualQuantileHist = Histogram(1,1000)
      val predictedQuantileHist = Histogram(1,1000)

      println("interval, actualQuantile, predictedQuantile")

      (2 to numIntervals).foreach(i => {
        val actualHist = perIterationHistograms((myThoughts, i))
        val actualQuantile = actualHist.quantile(quantile)
        actualQuantileHist += actualQuantile

        val predictedQuantile = predictOneInterval(i, quantile)
        predictedQuantileHist += predictedQuantile

        println(List(i, actualQuantile, predictedQuantile).mkString(","))
      })

      (actualQuantileHist, predictedQuantileHist)
    }
  }
  
  object ModelThoughtstream {
    import ScadrData._
    import TpcwModeling.Util._
    
    val thoughtstream = QueryDescription("thoughtstream", List(100, 50), 50)
    val thoughtstreamHist = histogramsScadr(thoughtstream)
    
    //scala> res1.thoughtstream.physicalPlan  
    //res5: edu.berkeley.cs.scads.piql.QueryPlan = LocalStopAfter(ParameterLimit(1,10000),
    //                                             IndexMergeJoin(<Namespace: thoughts>,ArrayBuffer(AttributeValue(0,1)),List(AttributeValue(1,1)),ParameterLimit(1,10000),false,
    //                                             IndexScan(<Namespace: subscriptions>,ArrayBuffer(ParameterValue(0)),FixedLimit(10000),true)))
    
    val indexMergeJoinThoughts = QueryDescription("indexMergeJoinThoughts", List(100, 50), 50)
    val indexMergeJoinThoughtsHist = histogramsScadr(indexMergeJoinThoughts).buckets.map(BigInt(_))
    
    val indexScanSubscriptions = QueryDescription("indexScanSubscriptions", List(100), 100)
    val indexScanSubscriptionsHist = histogramsScadr(indexScanSubscriptions).buckets.map(BigInt(_))
    
    def predictHist = {
      val res = convolve(indexScanSubscriptionsHist, indexMergeJoinThoughtsHist)
      res
    }
    
    val actual99th = thoughtstreamHist.quantile(0.99)
    val predicted99th = quantile(predictHist, 0.99)
    
    def predictOneInterval(i: Int, desiredQuantile: Double): Int = {
      val indexScanSubscriptionsHist = perIterationHistograms((indexScanSubscriptions, i)).buckets.map(BigInt(_))
      val indexMergeJoinThoughtsHist = perIterationHistograms((indexMergeJoinThoughts, i)).buckets.map(BigInt(_))
      
      val res = convolve(indexScanSubscriptionsHist, indexMergeJoinThoughtsHist)

      quantile(res, desiredQuantile)
    }
    
    def getPerIntervalPrediction(quantile: Double = 0.99):(Histogram, Histogram) = {
      val actualQuantileHist = Histogram(1,1000)
      val predictedQuantileHist = Histogram(1,1000)
      
      println("interval, actualQuantile, predictedQuantile")
      
      (2 to numIntervals).foreach(i => {
        val actualHist = perIterationHistograms((thoughtstream, i))
        val actualQuantile = actualHist.quantile(quantile)
        actualQuantileHist += actualQuantile
        
        val predictedQuantile = predictOneInterval(i, quantile)
        predictedQuantileHist += predictedQuantile
        
        println(List(i, actualQuantile, predictedQuantile).mkString(","))
      })
      
      (actualQuantileHist, predictedQuantileHist)
    }
    
  }
  
  object ModelUsersFollowedBy {
    import ScadrData._
    import TpcwModeling.Util._
    
    val usersFollowedBy = QueryDescription("usersFollowedBy", List(50), 50) // TODO:  choose cardinality here
    val usersFollowedByHist = histogramsScadr(usersFollowedBy)
    
    //scala> res1.usersFollowedBy.physicalPlan
    //res4: edu.berkeley.cs.scads.piql.QueryPlan = IndexLookupJoin(<Namespace: users>,ArrayBuffer(AttributeValue(0,1)),
    //                                             LocalStopAfter(ParameterLimit(1,10000),
    //                                             IndexScan(<Namespace: subscriptions>,ArrayBuffer(ParameterValue(0)),ParameterLimit(1,10000),true)))
    
    val indexLookupJoinUsers = QueryDescription("indexLookupJoinUsers", List(50), 50)
    val indexLookupJoinUsersHist = histogramsScadr(indexLookupJoinUsers).buckets.map(BigInt(_))
    
    val indexScanSubscriptions = QueryDescription("indexScanSubscriptions", List(50), 50)
    val indexScanSubscriptionsHist = histogramsScadr(indexScanSubscriptions).buckets.map(BigInt(_))
    
    def predictHist = {
      val res = convolve(indexScanSubscriptionsHist, indexLookupJoinUsersHist)
      res
    }
    
    val actual99th = usersFollowedByHist.quantile(0.99)
    val predicted99th = quantile(predictHist, 0.99)

    def predictOneInterval(i: Int, desiredQuantile: Double): Int = {
      val indexScanSubscriptionsHist = perIterationHistograms((indexScanSubscriptions, i)).buckets.map(BigInt(_))
      val indexLookupJoinUsersHist = perIterationHistograms((indexLookupJoinUsers, i)).buckets.map(BigInt(_))
      
      val res = convolve(indexScanSubscriptionsHist, indexLookupJoinUsersHist)

      quantile(res, desiredQuantile)
    }
    
    def getPerIntervalPrediction(quantile: Double = 0.99):(Histogram, Histogram) = {
      val actualQuantileHist = Histogram(1,1000)
      val predictedQuantileHist = Histogram(1,1000)
      
      println("interval, actualQuantile, predictedQuantile")
      
      (2 to numIntervals).foreach(i => {
        val actualHist = perIterationHistograms((usersFollowedBy, i))
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