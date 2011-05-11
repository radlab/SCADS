package edu.berkeley.cs.scads.storage

import edu.berkeley.cs.avro.runtime.ScalaSpecificRecord
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
  with KeyRangeRoutable {
   
  def applyAggregate(groups:Seq[String],
                     keyType:String,
                     valType:String,
                     filters:Option[AggFilters],
                     aggs:Seq[AggOp],
                     remType:ScalaSpecificRecord): Unit = {
    println(valueSchema)
    val aggRequest = AggRequest(groups,keyType, valType, filters,aggs)
    val partitions = serversForKeyRange(None,None)
    val responses = partitions.map(_.servers.map(_ !! aggRequest))
    new AggHandler(responses,remType)
  }


  class AggHandler(val futures:Seq[Seq[MessageFuture]], val remType:ScalaSpecificRecord, val timeout:Long = 5000) {
    private val responses = new java.util.concurrent.LinkedBlockingQueue[MessageFuture]
    futures.foreach(_.foreach(_.forward(responses)))
    val startTime = System.currentTimeMillis

    val future = responses.poll(timeout,TimeUnit.MILLISECONDS)
    if (future == null) 
      logger.info("FAILED")
    else {
      future() match {
        case AggReply(results) => {
          results.foreach (result => {
            remType.parse(result.groupVals(0))
            println(remType)
          })
        }
        case m => throw new RuntimeException("Unexpected Message: "+m)
      }
    }
  }
}
