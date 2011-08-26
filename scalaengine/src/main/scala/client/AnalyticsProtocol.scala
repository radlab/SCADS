package edu.berkeley.cs.scads.storage

import edu.berkeley.cs.avro.runtime.ScalaSpecificRecord
import edu.berkeley.cs.avro.marker._
import edu.berkeley.cs.scads.comm._
import edu.berkeley.cs.scads.util._

import scala.tools.nsc.Interpreter._

import org.apache.avro.Schema 
import org.apache.avro.generic._

import actors.Actor
import collection.mutable.{ Seq => MutSeq, _ }
import concurrent.ManagedBlocker

import java.util.concurrent.TimeUnit
import java.io.{ByteArrayOutputStream,ObjectOutputStream}

trait AnalyticsProtocol
  extends QuorumProtocol 
  with KeyRangeRoutable {
   
  def applyAggregate
    (groups:Seq[String],
     keyType:String,
     valType:String,
     filtFuncs:Seq[Filter[_]],
     aggClasses:Seq[(LocalAggregate[_,_],RemoteAggregate[_,_,_])]):Seq[(GenericData.Record,Seq[_])] = {
      val filters = filtFuncs.map(func => {
        val baos = new ByteArrayOutputStream
        val oos = new ObjectOutputStream(baos)
        oos.writeObject(func)
        AggFilter(baos.toByteArray)
      })
      val aggs = aggClasses.map(ac => {
        val baos = new ByteArrayOutputStream
        val oos = new ObjectOutputStream(baos)
        oos.writeObject(ac._2)
        AggOp(ac._2.getClass.getName,AnalyticsUtils.getClassBytes(ac._2),baos.toByteArray,false)
      })
      val aggRequest = AggRequest(groups,keyType, valType,filters,aggs)
      val partitions = serversForKeyRange(None,None)
      val partitionMap = new HashMap[String,String]
      val responses = partitions.map(partition => {
        partition.servers.map(server => {
          partitionMap += ((server.toString,server.partitionId))
          server !! aggRequest
        })
      })
      val groupSchema = AnalyticsUtils.getSubSchema(groups,valueSchema)
      val handler = new AggHandler(responses,partitionMap,partitions.size,aggClasses.map(_._1).zipWithIndex,groupSchema,partitions.size)
      handler.getReplies()
      handler.finish()
    }


  class AggHandler
    (val futures:Seq[Seq[MessageFuture]],
     val partitionMap:HashMap[String,String],
     val numParts:Int,
     val aggs:Seq[(LocalAggregate[_,_],Int)],
     val groupSchema:Schema,
     val numRanges:Int,
     val timeout:Long = 60000) {

      private val responses = new java.util.concurrent.LinkedBlockingQueue[MessageFuture]
      private val partitionReplies = new HashMap[String,AggReply]
      private val timeoutCounter = new TimeoutCounter(timeout)
      
      def getReplies():Unit = {
        futures.foreach(_.foreach(_.forward(responses)))
        val startTime = System.currentTimeMillis
        
        while (partitionReplies.size < numParts) {
          val future = 
            if (timeout > 0)
              responses.poll(timeoutCounter.remaining,TimeUnit.MILLISECONDS)
            else
              responses.take()
          if (future == null) 
            logger.info("FAILED")
          else {
            future.source match {
              case Some(rap) => {
                val pstr = rap.toString
                val pid = partitionMap(pstr)
                if (!partitionReplies.contains(pstr)) { // this is a partition we haven't seen yet
                  future() match {
                    case a:AggReply => partitionReplies += ((pstr,a))
                    case m => throw new RuntimeException("Unexpected Message: "+m)
                  }
                }
              }
              case None => println("Error, no source")
            }
          }
        }
      }

    
      def finish():Seq[(GenericData.Record,Seq[_])] = {
        val replyMap = new HashMap[GenericData.Record,ArrayBuilder[Array[Byte]]]
        partitionReplies foreach (pr => {
          pr._2.results foreach (result => {
            val groupRec = result.group match {
              case None => null
              case Some(bytes) => AnalyticsUtils.getRecord(groupSchema,bytes)
            }
            try {
              aggs foreach (ap => {
                ap._1.addForGroup(groupRec,result.groupVals(ap._2))
              })
            } catch {
              case e:Exception => {e.printStackTrace()}
            }
          })
        })
        val grps = aggs(0)._1.groups // all aggs have same groups
        if (grps.size == 0) { // not grouping
          List(
            (null,
             aggs.map(ap => {
               ap._1.finishForGroup(null)
             })
           )
          )
        } else {
          grps.map (group => {
            ( 
              group,
              aggs.map (ap => {
                ap._1.finishForGroup(group)
              }) 
            )
          }).toSeq
        }
      }
    }
}
        

        //results.foreach (result => {

          //            aggs foreach(ap => {
          //              val aggReply = ap._1.replyInstance
          //              aggReply.parse(result.groupVals(ap._2))
          //              aggReplies = aggReply :: aggReplies                
          //            })
          //          })
          //        }

        //val f = aggReplies.foldLeft(aggClasses(0).localAggregate.init())(aggClasses(0).localAggregate.foldFunction)
        //aggClasses(0).localAggregate.finalize(f)



