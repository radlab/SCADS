package edu.berkeley.cs.scads.deployment

import deploylib._

import org.apache.log4j.Logger

import edu.berkeley.cs.scads.thrift._
import edu.berkeley.cs.scads.model._
import edu.berkeley.cs.scads.placement._
import edu.berkeley.cs.scads.keys.{MinKey,MaxKey,Key,KeyRange,StringKey}
import edu.berkeley.cs.scads.nodes._

case class BlockingTriesExceededException(e:String) extends Exception(e)

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

    val maxBlockingTries = 1000
    val logger = Logger.getLogger("ScadsUtil")

    def blockUntilRunning(runitService: Service):Unit = {
        var i = 0
        while( !runitService.status.trim.equals("run") ) {
            if ( i == maxBlockingTries ) {
                val msg = "Exceeded max blocking tries"
                logger.fatal(msg)
                throw new BlockingTriesExceededException(msg)
            }
            logger.info("got status '" + runitService.status + "', expecting 'run'")
            runitService.start // keep trying!
            Thread.sleep(1000);// try to mitigate busy-wait
            i += 1
        }
    }

}
