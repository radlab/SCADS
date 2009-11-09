package edu.berkeley.cs.scads.deployment

import edu.berkeley.cs.scads.thrift._
import edu.berkeley.cs.scads.model._
import edu.berkeley.cs.scads.placement._
import edu.berkeley.cs.scads.keys.{MinKey,MaxKey,Key,KeyRange,StringKey}
import edu.berkeley.cs.scads.nodes._

object ScadsDeployUtil {

    def getAllRangeRecordSet():RecordSet = {
        new RecordSet(RecordSetType.RST_RANGE, new RangeSet(), null, null)
    }

    def getNthElemRS(n:Int):RecordSet = {
        val rset = new RecordSet()
        rset.setType(RecordSetType.RST_RANGE)
        val range = new RangeSet()
        rset.setRange(range)
        range.setOffset(n)
        range.setLimit(1)
        rset
    }

    /**
     * Only copies start/end keys, the rest is up to you
     */
    def getRS(rs:RecordSet):RecordSet = {
        getRS(rs.range.getStart_key,rs.range.getEnd_key)
    }

    def getRS(start:String,end:String):RecordSet = {
        val rset = new RecordSet()
        rset.setType(RecordSetType.RST_RANGE)
        val range = new RangeSet()
        rset.setRange(range)
        range.setStart_key(start)
        range.setEnd_key(end)
        rset
    }

}

object ListConversions {
    def scala2JavaList[T](l:List[T]) = java.util.Arrays.asList(l.toArray: _*)
}

