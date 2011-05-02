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

object TpcwModeling {
  object TpcwData {
    def experimentResults = allResults.filter(_.clientConfig.clusterAddress == "zk://zoo.knowsql.org/home/kcurtis/scads/experimentCluster0000000008")
    
    def goodExperimentResults = experimentResults.filterNot(_.iteration == 1)
    
    val histogramsTpcw = queryTypeHistogram(goodExperimentResults.toSeq)
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
    val indexScanOrdersIdxHist = histogramsTpcw(indexScanOrdersIdx)
    
    val indexLookupJoinOrders = QueryDescription("indexLookupJoinOrders", List(), 1)
    val indexLookupJoinOrdersHist = histogramsTpcw(indexLookupJoinOrders)
    
    //val indexLookupJoinAddresses  // didn't get executed
    val indexLookupJoinAddresses = QueryDescription("indexLookupJoinAddresses", List(), 1)
    val indexLookupJoinAddressesHist = histogramsTpcw(indexLookupJoinAddresses)
    
    val indexLookupJoinCountries = QueryDescription("indexLookupJoinCountries", List(), 1)
    val indexLookupJoinCountriesHist = histogramsTpcw(indexLookupJoinCountries)
  }
}