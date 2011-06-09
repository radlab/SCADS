package edu.berkeley.cs.scads.matheon

import edu.berkeley.cs.avro.marker.AvroRecord
import edu.berkeley.cs.scads.comm._
import edu.berkeley.cs.scads.util._

// container class for matheon data
case class MReading(var mass:Double, var count:Int) extends AvroRecord

// Here we define the custom aggregate that will pick peaks

// container class, holds the peaks we've found and some running data
case class PeakContainer(var lastZero:Double,var maxHeight:Int,var peaks:Seq[Double]) extends AvroRecord

class PeaksRemote(min_peak_width:Double,min_peak_height:Int) extends RemoteAggregate[PeakContainer, IntRec, MReading] {

  def init():PeakContainer = {
    PeakContainer(-1.0,0,List[Double]())
  }

  def applyAggregate(pc:PeakContainer, key:IntRec, reading:MReading):PeakContainer = {
    if (reading.count == 0) {
      if (pc.lastZero > 0) {
        val width = reading.mass - pc.lastZero 
        if (width >= min_peak_width &&
            pc.maxHeight > min_peak_height)
          pc.peaks = pc.peaks :+ ((reading.mass+pc.lastZero) / 2)
      }
      pc.lastZero = reading.mass
      pc.maxHeight = 0
    } else {
      if (reading.count > pc.maxHeight)
        pc.maxHeight = reading.count
    }
    pc
  }
}

class PeaksLocal extends LocalAggregate[PeakContainer, Seq[Double]] {
  def init():PeakContainer = {
    PeakContainer(-1.0,0,List[Double]())
  }
  def foldFunction(cur:PeakContainer, next: PeakContainer): PeakContainer = {
    cur.peaks = cur.peaks ++ next.peaks
    cur
  }
  def finalize(pc:PeakContainer):Seq[Double] = {
    pc.peaks
  }
}
