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

object ModelingExperiments {
  object ModelThoughtstreamWithGenericBenchmarks {
    import ExperimentUtil._
    
    def genericBenchmarkResultsDataSize30 = allResults.filter(_.clientConfig.clusterAddress == "zk://zoo.knowsql.org/home/marmbrus/scads/experimentCluster0000000176").filterNot(_.iteration == 1)
    def genericBenchmarkResultsDataSize40 = allResults.filter(_.clientConfig.clusterAddress == "zk://zoo.knowsql.org/home/marmbrus/scads/experimentCluster0000000177").filterNot(_.iteration == 1)
    
    val thoughtstream = QueryDescription("thoughtstream", List(100,50))
    val indexScanBenchmark = QueryDescription("indexScanQuery", List(100))
    val indexMergeJoinBenchmark = QueryDescription("indexMergeJoin", List(100,50))
    
    val histograms30 = queryTypeHistogram(genericBenchmarkResultsDataSize30.toSeq)
    val histograms40 = queryTypeHistogram(genericBenchmarkResultsDataSize40.toSeq)
    val histogramsScadr = queryTypeHistogram(goodExperimentResults.toSeq)

    val indexScanHist = histograms30(indexScanBenchmark)
    val indexMergeJoinHist = histograms40(indexMergeJoinBenchmark)
    val thoughtstreamHist = histogramsScadr(thoughtstream)
  }
}