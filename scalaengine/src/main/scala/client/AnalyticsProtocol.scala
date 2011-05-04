package edu.berkeley.cs.scads.storage

import edu.berkeley.cs.avro.marker._
import edu.berkeley.cs.scads.comm._

import org.apache.avro.Schema 
import org.apache.avro.generic._

import actors.Actor
import collection.mutable.{ Seq => MutSeq, _ }
import concurrent.ManagedBlocker

import java.util.concurrent.TimeUnit

trait AnalyticsProtocol 
  extends QuorumProtocol 
  with KeyPartitionable {
    
  def applyAggregate(groups:Seq[String],
                     filters:Option[AggFilters]): Unit = {
    val aggRequest = AggRequest(groups,filters)
    val ranges = routingTable.ranges map (_.values)
    ranges foreach (_ foreach (_ !! aggRequest))
  }
}
