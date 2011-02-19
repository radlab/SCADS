package edu.berkeley.cs.scads.director

import net.lag.logging.Logger

abstract class WorkloadPrediction {
	def addHistogram(histogram:WorkloadHistogram, time:Long)
	def addHistogram(histogram:WorkloadHistogram, previousHistogram:WorkloadHistogram, time:Long)
	def getPrediction():WorkloadHistogram
	def getCurrentSmoothed():WorkloadHistogram
	def initialize
	def getParams:Map[String,String]
}

case class SimpleHysteresis(
	alpha_up:Double,
	alpha_down:Double,
	overprovision:Double
) extends WorkloadPrediction {
	var prediction: WorkloadHistogram = null

	var fRaw:java.io.FileWriter = null
	var fSmooth:java.io.FileWriter = null
	var loggedHeaders = false

	def initialize { 
		prediction = null
		fRaw = new java.io.FileWriter(Director.basedir+"/workloadprediction_raw.csv")
		fSmooth = new java.io.FileWriter(Director.basedir+"/workloadprediction_smooth.csv")
	}

  def getCurrentSmoothed():WorkloadHistogram = prediction

	def addHistogram(histogram:WorkloadHistogram, time:Long) {
		if (!loggedHeaders) {
			fRaw.write( "time,"+histogram.toCSVHeader + "\n" )
			fSmooth.write( "time,"+histogram.toCSVHeader + "\n" )
			loggedHeaders = true
		}

		if (prediction==null) prediction = histogram
		else prediction = prediction.doHysteresis(histogram,alpha_up,alpha_down)

		if (histogram!=null) { fRaw.write( time+","+histogram.toCSVWorkload+"\n"); fRaw.flush }
		if (prediction!=null) { fSmooth.write( time+","+prediction.toCSVWorkload+"\n"); fSmooth.flush }

		//else if (histogram > prediction) prediction = prediction + (histogram - prediction)*alpha_up
		//else prediction = prediction + (histogram - prediction)*alpha_down
	}
  def addHistogram(histogram:WorkloadHistogram, previousHistogram:WorkloadHistogram, time:Long) = {
    // convert the current prediction, if it exists, to the same partitions as the incoming histogram
    // have: map of oldpart -> set(new parts), oldpart -> set(merged in parts)

    // now do hysteresis
    if (!loggedHeaders) {
			fRaw.write( "time,"+histogram.toCSVHeader + "\n" )
			fSmooth.write( "time,"+histogram.toCSVHeader + "\n" )
			loggedHeaders = true
		}

		if (prediction==null) prediction = histogram
		else prediction = previousHistogram.doHysteresis(histogram,alpha_up,alpha_down)

		if (histogram!=null) { fRaw.write( time+","+histogram.toCSVWorkload+"\n"); fRaw.flush }
		if (prediction!=null) { fSmooth.write( time+","+prediction.toCSVWorkload+"\n"); fSmooth.flush }
  }

	def getPrediction():WorkloadHistogram = { try{prediction*(1+overprovision)} catch {case _ => null} }
	def getParams:Map[String,String] = Map("alpha_up"->alpha_up.toString,"alpha_down"->alpha_down.toString,"safety"->overprovision.toString)
}
