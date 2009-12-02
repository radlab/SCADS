package edu.berkeley.cs.scads.deployment

import edu.berkeley.cs.scads.thrift.{Record,RecordSet,StorageNode}

import scala.collection.jcl.Conversions


class DeployStorageNode(hostname: String, thriftPort: Int) extends StorageNode(hostname, thriftPort) {

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
