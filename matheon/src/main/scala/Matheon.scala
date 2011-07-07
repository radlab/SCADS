package edu.berkeley.cs.scads.matheon

import edu.berkeley.cs.avro.marker.AvroRecord
import edu.berkeley.cs.scads.comm._
import edu.berkeley.cs.scads.util._

case class MatheonKey(var readingId:Int) extends AvroRecord

// container class for matheon data
case class MReading(var fileId:Int, var mass:Float, var count:Float) extends AvroRecord

// Here we define the custom aggregate that will pick peaks

// container class, holds the peaks we've found and some running data
case class PeakContainer(var lastZero:Float,var maxHeight:Float,var peaks:Seq[Float]) extends AvroRecord

class PeaksRemote(min_peak_width:Float,min_peak_height:Float) extends RemoteAggregate[PeakContainer, MatheonKey, MReading] {

  def init():PeakContainer = {
    PeakContainer(-1.0f,0,List[Float]())
  }

  def applyAggregate(pc:PeakContainer, key:MatheonKey, reading:MReading):PeakContainer = {
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

class PeaksLocal extends LocalAggregate[PeakContainer, Seq[Float]] {
  def init():PeakContainer = {
    PeakContainer(-1.0f,0,List[Float]())
  }
  def foldFunction(cur:PeakContainer, next: PeakContainer): PeakContainer = {
    cur.peaks = cur.peaks ++ next.peaks
    cur
  }
  def finalize(pc:PeakContainer):Seq[Float] = {
    pc.peaks
  }
}
