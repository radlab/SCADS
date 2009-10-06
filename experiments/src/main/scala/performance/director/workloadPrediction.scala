package scads.director

import performance._
import java.util.Date



abstract class WorkloadPrediction {
	def addHistogram(histogram:WorkloadHistogram)
	def getPrediction():WorkloadHistogram
	def initialize
	def getParams:Map[String,String]
}

case class SimpleHysteresis(
	alpha_up:Double,
	alpha_down:Double,
	overprovision:Double
) extends WorkloadPrediction {
	var prediction: WorkloadHistogram = null
	
	def initialize { prediction = null }
	
	def addHistogram(histogram:WorkloadHistogram) {
		if (prediction==null) prediction = histogram
		else if (histogram > prediction) prediction = prediction + (histogram - prediction)*alpha_up
		else prediction = prediction + (histogram - prediction)*alpha_down
	}
	
	def getPrediction():WorkloadHistogram = { try{prediction*(1+overprovision)} catch {case _ => null} }
	def getParams:Map[String,String] = Map("alpha_up"->alpha_up.toString,"alpha_down"->alpha_down.toString,"overprovision"->overprovision.toString)
}

case class IdentityPrediction extends WorkloadPrediction {
	var prediction:WorkloadHistogram = null
	def initialize { prediction = null }
	def addHistogram(histogram:WorkloadHistogram) { prediction = histogram }
	def getPrediction():WorkloadHistogram = prediction
	def getParams:Map[String,String] = Map[String,String]()
}

case class MeanPlusVariancePrediction(
	windowLength:Int,
	nStdevs:Int
) extends WorkloadPrediction {
	var histogramWindow = new scala.collection.mutable.ListBuffer[WorkloadHistogram]()
	
	def initialize { histogramWindow = new scala.collection.mutable.ListBuffer[WorkloadHistogram]() }
	
	def addHistogram(histogram:WorkloadHistogram) {
		if (histogramWindow.size >= windowLength) { histogramWindow.remove(0) }
		histogramWindow += histogram
	}
	
	def getPrediction():WorkloadHistogram = {
		null
	}

	def getParams:Map[String,String] = Map("windowLength"->windowLength.toString,"nStdevs"->nStdevs.toString)
}