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
  
  object ModelThoughtstreamWithScadrBenchmarks {
    import ScadrData._

    val scadrIndexScanBenchmark = QueryDescription("scadrIndexScanBenchmark", List(100))
    val scadrIndexMergeJoinBenchmark = QueryDescription("scadrIndexMergeJoinBenchmark", List(100,50))
    
    val scadrIndexScanHist = histogramsScadr(scadrIndexScanBenchmark)
    val scadrIndexMergeJoinHist = histogramsScadr(scadrIndexMergeJoinBenchmark)
  }
  
  object TpcwData {
    def experimentResults = allResults.filter(_.clientConfig.clusterAddress == "zk://zoo.knowsql.org/home/marmbrus/scads/experimentCluster0000000191").filterNot(_.iteration == 1)
    
    val tpcwQueryTypes = queryTypes(experimentResults.toSeq)
  }
}