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


/**
* use linear regression on the aggregate workload rate to forecast workload
*/
case class ForecastWithHysteresis(
	alpha_up:Double,
	alpha_down:Double,
	overprovision:Double,
	forecastDataInterval:Long,
	forecastInterval:Long
) extends WorkloadPrediction {
	var prediction: WorkloadHistogram = null
	
	var workloadData = scala.collection.mutable.Map[Long,Double]()
	var workloadModel:LinearRegression1D = null
	
	def initialize { 
		prediction = null 
		workloadData = scala.collection.mutable.Map[Long,Double]()
	}
	
	def addHistogram(histogram:WorkloadHistogram) {
		//TODO: fix the time, use policy time instead real time (will not work in simulation)
		val now = new java.util.Date().getTime
		workloadData += now -> histogram.totalRate
		workloadData = scala.collection.mutable.Map( workloadData.toList.filter(_._1>now-forecastDataInterval):_* )
		
		if (workloadData.size>=5)
			workloadModel = LinearRegression1D( workloadData.toList.sort(_._1<_._1).map(_._1-now), workloadData.toList.sort(_._1<_._1).map(_._2) )
		else {
			workloadModel = null
			Director.logger.info("don't have workload forecast model")
		}
			
		val forecastedHistogram = if (workloadModel!=null) {
								val w0 = histogram.totalRate
								val wp = workloadModel.getPrediction(forecastInterval)
								Director.logger.info("have workload forecast model. ratio = "+wp+"/"+w0+" = "+(wp/w0))
								histogram * (wp / w0)
							} else histogram
		
		if (prediction==null) prediction = forecastedHistogram
		else if (forecastedHistogram > prediction) prediction = prediction + (forecastedHistogram - prediction)*alpha_up
		else prediction = prediction + (forecastedHistogram - prediction)*alpha_down
	}
	
	def getPrediction():WorkloadHistogram = { try{prediction*(1+overprovision)} catch {case _ => null} }
	def getParams:Map[String,String] = Map("alpha_up"->alpha_up.toString,"alpha_down"->alpha_down.toString,"overprovision"->overprovision.toString,
											"forecastDataInterval"->forecastDataInterval.toString,"forecastInterval"->forecastInterval.toString)
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


case class LinearRegression1D(
	x: List[Double],
	y: List[Double]
) {
	assert(x.size==y.size,"length of x and y must be the same")
	assert(x.size>1,"need at least 2 datapoints")
	
	val (alpha0,alphax) = fit
	
	def getPrediction(xnew:Double):Double = alpha0 + alphax*xnew
	
	def fit:(Double,Double) = {
		Director.logger.debug("got data for workload forecast:\nx="+x+"\ny="+y)
		
		val sumx = x.reduceLeft(_+_)
		val sumx2 = x.map(xi=>xi*xi).reduceLeft(_+_)
		val sumy = y.reduceLeft(_+_)
		val sumxy = x.zip(y).map(xyi=>xyi._1*xyi._2).reduceLeft(_+_)
		val n = x.size
		
		val cx = (n*sumxy - sumy*sumx)/(n*sumx2-sumx*sumx)
		val c0 = sumy/n - cx*sumx/n
		(c0,cx)
	}
}