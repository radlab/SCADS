package scads.director

import performance._
import java.util.Date



abstract class WorkloadPrediction {
	def addHistogram(histogram:WorkloadHistogram)
	def getPrediction():WorkloadHistogram
}

case class SimpleHysteresis(
	alpha_up:Double,
	alpha_down:Double
) extends WorkloadPrediction {
	var prediction: WorkloadHistogram = null
	
	def addHistogram(histogram:WorkloadHistogram) {
		if (prediction==null) prediction = histogram
		else if (histogram > prediction) prediction = prediction + (histogram - prediction)*alpha_up
		else prediction = prediction + (histogram - prediction)*alpha_down
	}
	
	def getPrediction():WorkloadHistogram = prediction	
}

case class IdentityPrediction extends WorkloadPrediction {
	var prediction:WorkloadHistogram = null
	def addHistogram(histogram:WorkloadHistogram) { prediction = histogram }
	def getPrediction():WorkloadHistogram = prediction
}

case class MeanPlusVariancePrediction(
	windowLength:Int,
	nStdevs:Int
) extends WorkloadPrediction {
	var histogramWindow = new scala.collection.mutable.ListBuffer[WorkloadHistogram]()
	
	def addHistogram(histogram:WorkloadHistogram) {
		if (histogramWindow.size >= windowLength) { histogramWindow.remove(0) }
		histogramWindow += histogram
	}
	
	def getPrediction():WorkloadHistogram = {
		null
	}
	
}