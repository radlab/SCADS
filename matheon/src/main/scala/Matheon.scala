package edu.berkeley.cs.scads.matheon

import edu.berkeley.cs.avro.marker.AvroRecord
import edu.berkeley.cs.scads.comm._
import edu.berkeley.cs.scads.util._

case class MatheonKey(var readingId:Int) extends AvroRecord

// container class for matheon data
case class MReading(var fileId:Int, var mass:Float, var count:Float) extends AvroRecord

// Here we define the custom aggregates that will pick peaks

// First, a simple, find the point between two zeros
// container class, holds the peaks we've found and some running data
case class ZeroPeakContainter(var lastZero:Float,var maxHeight:Float,var peaks:Seq[Float]) extends AvroRecord

// function to execute remotly
class ZeroPeaksRemote(min_peak_width:Float,min_peak_height:Float) extends RemoteAggregate[ZeroPeakContainter, MatheonKey, MReading] {

  def init():ZeroPeakContainter = {
    ZeroPeakContainter(-1.0f,0,List[Float]())
  }

  def applyAggregate(pc:ZeroPeakContainter, key:MatheonKey, reading:MReading):ZeroPeakContainter = {
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

// function to execute locally
class ZeroPeaksLocal extends LocalAggregate[ZeroPeakContainter, Seq[Float]] {
  def init():ZeroPeakContainter = {
    ZeroPeakContainter(-1.0f,0,List[Float]())
  }
  def foldFunction(cur:ZeroPeakContainter, next: ZeroPeakContainter): ZeroPeakContainter = {
    cur.peaks = cur.peaks ++ next.peaks
    cur
  }
  def finalize(pc:ZeroPeakContainter):Seq[Float] = {
    pc.peaks
  }
}



// next, a more complex picker that looks at the values around it to determine if it's a local peak
// container class for running values

case class DetPeakContainer(var minVal:Float, var maxVal:Float, var maxMass:Float, var lookMax:Boolean, var peakMass:Seq[Float], var peakCount:Seq[Float]) extends AvroRecord

//remote agg
class DetPeaksRemote(delta:Float) extends RemoteAggregate[DetPeakContainer, MatheonKey, MReading] {
  def init():DetPeakContainer = {
    DetPeakContainer(Float.MaxValue,Float.MinValue,0.0f,true,List[Float](),List[Float]())
  }

  def applyAggregate(pc:DetPeakContainer, key:MatheonKey, reading:MReading):DetPeakContainer = {
    if (reading.count > pc.maxVal) {
      pc.maxVal = reading.count
      pc.maxMass = reading.mass
    }

    if (reading.count < pc.minVal) 
      pc.minVal = reading.count

    if (pc.lookMax) {
      if (reading.count < (pc.maxVal - delta)) {
        pc.peakMass = pc.peakMass :+ (pc.maxMass)
        pc.peakCount = pc.peakCount :+ (pc.maxVal)
        pc.minVal = reading.count
        pc.lookMax = false
      }
    }
    else if (reading.count > pc.minVal+delta) {
      pc.maxVal = reading.count
      pc.maxMass = reading.mass
      pc.lookMax = true
    }
    pc
  }
}

// function to execute locally
class DetPeaksLocal extends LocalAggregate[DetPeakContainer, (Seq[Float],Seq[Float])] {
  def init():DetPeakContainer = {
    DetPeakContainer(Float.MaxValue,Float.MinValue,0.0f,true,List[Float](),List[Float]())
  }
  def foldFunction(cur:DetPeakContainer, next: DetPeakContainer): DetPeakContainer = {
    cur.peakMass = cur.peakMass ++ next.peakMass
    cur.peakCount = cur.peakCount ++ next.peakCount
    cur
  }
  def finalize(pc:DetPeakContainer):(Seq[Float],Seq[Float]) = {
    (pc.peakMass,pc.peakCount)
  }
}
