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

object ModelingExperiments {
  object ScadrData {
    def experimentResults = allResults.filter(_.clientConfig.experimentAddress contains "experiment0000000164")

    def goodExperimentResults = experimentResults.filter(_.failedQueries < 200)
  			.filterNot(_.iteration == 1)
  			.filter(_.clientConfig.iterationLengthMin == 10)

    val histogramsScadr = queryTypeHistogram(goodExperimentResults.toSeq)

    val thoughtstream = QueryDescription("thoughtstream", List(100,50))
    val thoughtstreamHist = histogramsScadr(thoughtstream)
    
    val usersFollowedBy = QueryDescription("usersFollowedBy", List(100))  // need to re-benchmark this with (100 to 500 by 50) instead of (10 to 50 by 5)
  }

  object ModelThoughtstreamWithGenericBenchmarks {
    import ScadrData._
        
    def genericBenchmarkResultsDataSize30 = allResults.filter(_.clientConfig.clusterAddress == "zk://zoo.knowsql.org/home/marmbrus/scads/experimentCluster0000000176").filterNot(_.iteration == 1)
    def genericBenchmarkResultsDataSize40 = allResults.filter(_.clientConfig.clusterAddress == "zk://zoo.knowsql.org/home/marmbrus/scads/experimentCluster0000000177").filterNot(_.iteration == 1)
    
    val indexScanBenchmark = QueryDescription("indexScanQuery", List(100))
    val indexMergeJoinBenchmark = QueryDescription("indexMergeJoin", List(100,50))
    
    val histograms30 = queryTypeHistogram(genericBenchmarkResultsDataSize30.toSeq)
    val histograms40 = queryTypeHistogram(genericBenchmarkResultsDataSize40.toSeq)

    val indexScanHist = histograms30(indexScanBenchmark)
    val indexMergeJoinHist = histograms40(indexMergeJoinBenchmark)
  }
  
  object ModelUsersFollowedByWithScadrBenchmarks {
    import ScadrData._
    
    val scadrIndexScanBenchmark = QueryDescription("scadrIndexScanBenchmark", List(100))
    val scadrIndexLookupJoinBenchmark = QueryDescription("scadrIndexLookupJoinBenchmark", List(100))
    
    val predictedHist = histogramsScadr(scadrIndexScanBenchmark) convolveWith histogramsScadr(scadrIndexLookupJoinBenchmark)
  }
  
  object ModelThoughtstreamWithScadrBenchmarks {
    import ScadrData._

    val scadrIndexScanBenchmark = QueryDescription("scadrIndexScanBenchmark", List(100))
    val scadrIndexMergeJoinBenchmark = QueryDescription("scadrIndexMergeJoinBenchmark", List(100,50))
    
    val scadrIndexScanHist = histogramsScadr(scadrIndexScanBenchmark)
    val scadrIndexMergeJoinHist = histogramsScadr(scadrIndexMergeJoinBenchmark)
    
    val stddev = queryTypeStddev(goodExperimentResults.toSeq)
    
    def thoughtstreamPrediction = {
      println("numSubs, numPerPage, actual, predicted, difference, actualStddev")
      (100 to 500 by 50).foreach(numSubs => {
        (10 to 50 by 5).foreach(numPerPage => {
          val thoughtstream = QueryDescription("thoughtstream", List(numSubs, numPerPage))
          val thoughtstreamHist = histogramsScadr(thoughtstream)
          val scadrIndexScanHist = histogramsScadr(QueryDescription("scadrIndexScanBenchmark", List(numSubs)))
          val scadrIndexMergeJoinHist = histogramsScadr(QueryDescription("scadrIndexMergeJoinBenchmark", List(numSubs, numPerPage)))
          
          val predictedHist = scadrIndexScanHist convolveWith scadrIndexMergeJoinHist
          
          val actualStddev = stddev(thoughtstream)
          
          println(List(numSubs, numPerPage, thoughtstreamHist.quantile(0.99), predictedHist.quantile(0.99), predictedHist.quantile(0.99) - thoughtstreamHist.quantile(0.99), actualStddev.get).mkString(","))
        })
      })
    }
    
    // per-interval prediction
    def getNumIntervalsForGivenQueryDesc(givenQueryDesc: QueryDescription): Int = {
      val perIterationHistograms = queryTypePerIterationHistograms(goodExperimentResults.toSeq)
      
      val queryDescriptionsWithIterationNums = perIterationHistograms.keySet
      
      var max = 0
      
      queryDescriptionsWithIterationNums.map { 
        case(queryDesc, i) => 
          if (queryDesc == givenQueryDesc && i > max) 
            max = i 
      }
      
      max
    }
    
    def getThoughtstreamDistributionPredictionForGivenCardinality(numSubs: Int, numPerPage: Int) = {
      val thoughtstream = QueryDescription("thoughtstream", List(numSubs, numPerPage))
      val scadrIndexScan = QueryDescription("scadrIndexScanBenchmark", List(numSubs))
      val scadrIndexMergeJoin = QueryDescription("scadrIndexMergeJoinBenchmark", List(numSubs, numPerPage))

      val numIntervals = getNumIntervalsForGivenQueryDesc(thoughtstream)
      
      val perIterationHistograms = queryTypePerIterationHistograms(goodExperimentResults.toSeq)
      
      println("interval, actual99th, predicted99th")
      // skip first interval
      (2 to numIntervals).foreach(i => {
        val actualHist = perIterationHistograms((thoughtstream, i))
        
        val scadrIndexScanHist = perIterationHistograms((scadrIndexScan, i))
        val scadrIndexMergeJoinHist = perIterationHistograms((scadrIndexMergeJoin, i))
        
        val predictedHist = scadrIndexScanHist convolveWith scadrIndexMergeJoinHist
        
        println(List(i, actualHist.quantile(0.99), predictedHist.quantile(0.99)).mkString(","))
      })
    }
    
    def getThoughtstreamDistributionPredictionSummary(summaryType: String = "median", metaQuantile: Double = 0.5) = {
      println("numSubs, numPerPage, actualSummary, predictedSummary, difference, actualStddev, predictedStddev")
      (100 to 500 by 50).foreach(numSubs => {
        (10 to 50 by 5).foreach(numPerPage => {
          val (actual, predicted) = getActualAndPredictedHistogramsForGivenCardinality(numSubs, numPerPage, 0.99)

          var actualSummary = 0.0
          var predictedSummary = 0.0
          
          summaryType match {
            case "median" => {
              actualSummary = actual.median
              predictedSummary = predicted.median
            }
            case "mean" => {
              actualSummary = actual.average
              predictedSummary = predicted.average
            }
            case "quantile" => {
              actualSummary = actual.quantile(metaQuantile)
              predictedSummary = predicted.quantile(metaQuantile)
            }
          }

          println(List(numSubs, numPerPage, actualSummary, predictedSummary, predictedSummary - actualSummary, actual.stddev, predicted.stddev).mkString(","))
        })
      })
    }
    
    val perIterationHistograms = queryTypePerIterationHistograms(goodExperimentResults.toSeq)
    
    // want function that will give you a tuple with the actual, predicted histograms of the desired quantile (eg, 99th) for the given cardinality
    def getActualAndPredictedHistogramsForGivenCardinality(numSubs: Int, numPerPage: Int, quantile: Double = 0.99):(Histogram, Histogram) = {
      val thoughtstream = QueryDescription("thoughtstream", List(numSubs, numPerPage))
      val scadrIndexScan = QueryDescription("scadrIndexScanBenchmark", List(numSubs))
      val scadrIndexMergeJoin = QueryDescription("scadrIndexMergeJoinBenchmark", List(numSubs, numPerPage))
      val numIntervals = 30//getNumIntervalsForGivenQueryDesc(thoughtstream)

      val actualQuantileHist = Histogram(1,1000)
      val predictedQuantileHist = Histogram(1,1000)

      (2 to numIntervals).foreach(i => {
        val actualHist = perIterationHistograms((thoughtstream, i))
        
        val scadrIndexScanHist = perIterationHistograms((scadrIndexScan, i))
        val scadrIndexMergeJoinHist = perIterationHistograms((scadrIndexMergeJoin, i))
        
        val predictedHist = scadrIndexScanHist convolveWith scadrIndexMergeJoinHist

        actualQuantileHist += actualHist.quantile(quantile)
        predictedQuantileHist += predictedHist.quantile(quantile)
      })
      
      (actualQuantileHist, predictedQuantileHist)
    }
  }
}