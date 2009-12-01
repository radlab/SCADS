package edu.berkeley.cs.scads.deployment

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.log4j.Level._

import deploylib._
import deploylib.rcluster._
import deploylib.configuration._
import deploylib.configuration.ValueConverstion._

import edu.berkeley.cs.scads.thrift.{Record,RecordSet}
import edu.berkeley.cs.scads.model._
import edu.berkeley.cs.scads.placement._
import edu.berkeley.cs.scads.keys.{MinKey,MaxKey,Key,KeyRange,StringKey}
import edu.berkeley.cs.scads.nodes._

import org.apache.thrift.transport.{TFramedTransport, TSocket}
import org.apache.thrift.protocol.{TProtocol,TBinaryProtocol, XtBinaryProtocol}

import scala.collection.jcl.Conversions


class NamedStorageMachine(val hostname: String, thriftPort: Int, syncPort: Int) extends StorageNode(hostname, thriftPort, syncPort) {
    def this(h:String, p:Int) = this(h,p,p)


    def totalCount(ns:String):Int = { 
        val range = ScadsDeployUtil.getAllRangeRecordSet
        useConnection( (c) => c.count_set(ns,range) )
    }

    def totalSet(ns:String):List[Record] = {
        val range = ScadsDeployUtil.getAllRangeRecordSet
        Conversions.convertList(useConnection( (c) => c.get_set(ns,range) )).toList
    }

    def getNthRecord(ns:String,n:Int):Record = {
        val range = ScadsDeployUtil.getNthElemRS(n)
        val list = useConnection((c) => c.get_set(ns,range))
        if ( list.size == 0 ) {
            null
        } else { 
            list.get(0)
        }
    }

}
