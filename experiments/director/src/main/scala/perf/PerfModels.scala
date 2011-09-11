package edu.berkeley.cs.scads.director

import net.lag.logging.Logger
import java.util.Date
import scala.io._

object PerformanceModels {
	var steadystate:PerformanceModel = null
	var copyduration:CopyActionDurationModel = null
	var sourceCopyeffect:CopyActionEffectModel = null
	var targetCopyeffect:CopyActionEffectModel = null
	
	def initialize(repodir:String) {
		// create model of steady-state performance
		val modelfile = repodir+"/director/scripts/perfmodels/steadystate_model_get.csv"
		val throughputModelfile = repodir+"/director/scripts/perfmodels/throughput_model_get.csv"
		steadystate = LinearPerformanceModel(modelfile, throughputModelfile)

		// create model of copy duration
		val copyDurationModelPath = repodir+"/director/scripts/perfmodels/copyrate_model.csv"
		copyduration = CopyActionDurationModel(copyDurationModelPath)

		// create models of copy effects
		val sourceCopyEffectModelPath = repodir+"/director/scripts/perfmodels/copyeffect_model_source_get.csv"
		val targetCopyEffectModelPath = repodir+"/director/scripts/perfmodels/copyeffect_model_target_get.csv"
		sourceCopyeffect = CopyActionEffectModel(sourceCopyEffectModelPath)
		targetCopyeffect = CopyActionEffectModel(targetCopyEffectModelPath)		
	}
}

abstract class PerformanceModel {
	def overloaded(getWorkload:Double, putWorkload:Double): Boolean
	def sampleFractionSlow(getWorkload:Double, putWorkload:Double): Double
	def estimateFractionSlow(getWorkload:Double, putWorkload:Double): Double
	def slowThreshold(): Double
	
	def timeInModel():Long
	def resetTimer()

	val logger = Logger()
}

case class BinaryThroughputModel(
	modelURL:String
) {
	var coefficients = scala.collection.mutable.Map[String,Double]()
	initialize

	def initialize {
		val source = if (modelURL.startsWith("http://")) Source.fromURL(modelURL) else Source.fromFile(modelURL)
		val description = source.getLines.toList.head
		val s = description.split(":")
		coefficients = scala.collection.mutable.Map[String,Double]()
		s(1).split(",").foreach( k => coefficients += k.split("=")(0) -> k.split("=")(1).toDouble )
	}

	def overloaded(getw:Double, putw:Double):Boolean = {
		coefficients("(Intercept)") + getw*coefficients("get_workload") + putw*coefficients("put_workload") > 0
	}
}


case class LinearPerformanceModel(
	modelURL:String,
	throughputModelURL:String
) extends PerformanceModel {
	val throughputModel = BinaryThroughputModel(throughputModelURL)

	var coefficients:scala.collection.mutable.Map[String,Double] = null
	val rnd = new java.util.Random()
	initialize
	
	def slowThreshold:Double = 100
	
	var timer:Long = 0
	def resetTimer = { timer = 0 }
	def timeInModel:Long = timer
	
	def initialize() {
		val source = if (modelURL.startsWith("http://")) Source.fromURL(modelURL) else Source.fromFile(modelURL)
		val modelDescription = source.getLines.toList.head
		
		val s = modelDescription.split(":")
		coefficients = scala.collection.mutable.Map[String,Double]()
		s(1).split(",").foreach( k => coefficients += k.split("=")(0) -> k.split("=")(1).toDouble )
	}

	def overloaded(getW:Double, putW:Double):Boolean = throughputModel.overloaded(getW,putW)

	def estimateMeanFromModel(getW:Double, putW:Double):Double = {
		val mean =
			coefficients("(Intercept)") * 1.0 +
			(if (coefficients.contains("get_workload")) coefficients("get_workload")*getW else 0.0) +
			(if (coefficients.contains("put_workload")) coefficients("put_workload")*putW else 0.0)
		mean
	}
	
	def sampleGaussianError():Double = rnd.nextGaussian * coefficients("residual_sd")
	def sampleExponentialError():Double = {
		val expMean = coefficients("residual_mean") - coefficients("residual_min")
		-scala.math.log( rnd.nextDouble ) * expMean + coefficients("residual_min")
	}
	
	def estimateFractionSlow(getWorkload:Double, putWorkload:Double): Double = {
		val time0 = new Date
		var value = 
		if (overloaded(getWorkload,putWorkload)) 1.0
		else {
			val mean = estimateMeanFromModel(getWorkload, putWorkload)
			val bounded = scala.math.min( 1.0, scala.math.max(0.0, mean) )
			bounded
		}
		timer += (new Date().getTime-time0.getTime)
		value
	}

	def sampleFractionSlow(getWorkload:Double, putWorkload:Double): Double = {
		val time0 = new Date
		var value = 
		if (overloaded(getWorkload,putWorkload)) 1.0
		else {
			val mean = estimateMeanFromModel(getWorkload, putWorkload)
			val error = sampleExponentialError
			val bounded = scala.math.min( 1.0, scala.math.max(0.0, mean+error) )
			bounded
		}
		timer += (new Date().getTime-time0.getTime)
		value
	}
	
}



case class CopyActionDurationModel(
	modelURL:String
) {
	var coefficients:scala.collection.mutable.Map[String,Double] = null
	val rnd = new java.util.Random()
	initialize
	
	def initialize() {
		val source = if (modelURL.startsWith("http://")) Source.fromURL(modelURL) else Source.fromFile(modelURL)
		val modelDescription = source.getLines.toList.head
		
		val s = modelDescription.split(":")
		coefficients = scala.collection.mutable.Map[String,Double]()

		s(1).split(",").foreach( k =>
			coefficients += k.split("=")(0) -> k.split("=")(1).toDouble
		)
	}

	def estimateCopyRateFraction(getW:Double, putW:Double, copyRate:Double):Double = {
		val copyRateFraction = 
			coefficients("(Intercept)") * 1.0 +
			coefficients("get_w") * getW +
			coefficients("get_w_2") * getW*getW +
			coefficients("put_w") * putW +
			coefficients("put_w_2") * putW*getW +
			coefficients("copyrate") * copyRate/1e6 +
			coefficients("copyrate_2") * (copyRate/1e6)*(copyRate/1e6)
		val bounded = scala.math.min( 1.0, scala.math.max(0.0, copyRateFraction) )
		bounded
	}

	/**
	* estimate the duration of a copy action (in milliseconds).
	* getW = get request rate
	* putW = put request rate
	* copyRate = specified copy rate
	* dataSize = number of bytes to copy
	*/
	def estimateCopyDuration(getW:Double, putW:Double, copyRate:Double, dataSize:Long):Long = {
		val copyRateFraction = estimateCopyRateFraction(getW, putW, copyRate)
		val effectiveCopyRate = copyRate * copyRateFraction
		val duration = (dataSize.toDouble / effectiveCopyRate * 1000.0).toLong
		val bounded = scala.math.max(0L, duration)
		bounded
	}
	
	def sampleCopyDuration(getW:Double, putW:Double, copyRate:Double, dataSize:Long):Long = {
		val durationMean = estimateCopyDuration(getW, putW, copyRate, dataSize)
		val durationResidual = rnd.nextGaussian * coefficients("residual_sd")
		val durationSample = (durationMean + durationResidual).toLong
		val bounded = scala.math.max(0L, durationSample)
		bounded
	}
	
}


case class CopyActionEffectModel(
	modelURL:String
) {
	var coefficients:scala.collection.mutable.Map[String,Double] = null
	val rnd = new java.util.Random()
	initialize
	
	def initialize() {
		val source = if (modelURL.startsWith("http://")) Source.fromURL(modelURL) else Source.fromFile(modelURL)
		val modelDescription = source.getLines.toList.head
		
		val s = modelDescription.split(":")
		coefficients = scala.collection.mutable.Map[String,Double]()

		s(1).split(",").foreach( k =>
			coefficients += k.split("=")(0) -> k.split("=")(1).toDouble
		)
	}

	def estimateLogFracSlow1en4(getW:Double, putW:Double, copyRate:Double):Double = {
		val rateMBps = copyRate/1024.0/1024.0
		val logRateMBps = scala.math.log(rateMBps)
		
		val log_fracslow_1en4 = 		// model actually predicts "log( di$frac.slow.100+1e-4 )"
			coefficients("(Intercept)") * 1.0 +
			(if (coefficients.contains("get_w")) coefficients("get_w")*getW else 0.0) +
			(if (coefficients.contains("get_w_2")) coefficients("get_w_2")*getW*getW else 0.0) +
			(if (coefficients.contains("put_w")) coefficients("put_w")*putW else 0.0) +
			(if (coefficients.contains("put_w_2")) coefficients("put_w_2")*putW*putW else 0.0) +
			(if (coefficients.contains("rateMBps")) coefficients("rateMBps")*rateMBps else 0.0) +
			(if (coefficients.contains("log_rateMBps")) coefficients("log_rateMBps")*logRateMBps else 0.0)
		log_fracslow_1en4
	}
	
	def estimateFractionSlow(getW:Double, putW:Double, copyRate:Double):Double = {
		val log_fracslow_1en4 = estimateLogFracSlow1en4(getW, putW, copyRate)
		val fractionSlowMean = scala.math.exp(log_fracslow_1en4) - 1e-4
		val bounded = scala.math.min( 1.0, scala.math.max(0.0, fractionSlowMean) )
		bounded
	}
	
	def sampleFractionSlow(getW:Double, putW:Double, copyRate:Double):Double = {
		val log_fracslow_1en4_mean = estimateLogFracSlow1en4(getW, putW, copyRate)
		val log_fracslow_1en4_residual = rnd.nextGaussian * coefficients("residual_sd")
		val log_fracslow_1en4_sample = log_fracslow_1en4_mean + log_fracslow_1en4_residual
		val fracSlowSample = scala.math.exp( log_fracslow_1en4_sample ) - 1e-4
		val bounded = scala.math.min( 1.0, scala.math.max(0.0, fracSlowSample) )
		bounded
	}
}