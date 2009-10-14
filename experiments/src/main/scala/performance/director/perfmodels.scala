package scads.director

import java.util.Date
import scala.io._

import org.apache.log4j._
import org.apache.log4j.Level._

import org.rosuda._
import org.rosuda.REngine._
import org.rosuda.REngine.REXPMismatchException;
import org.rosuda.REngine.REngineException;
import org.rosuda.REngine.Rserve.RConnection;
import org.rosuda.REngine.Rserve.RserveException;


abstract class PerformanceModel {
	def sample(features:Map[String,String], nSamples:Int): List[Double]
	def estimateLatency(features:Map[String,String], quantile:Double):Double
	
	def timeInModel():Long
	def resetTimer()
	
	val logger = Logger.getLogger("scads.perfmodel")
	private val logPath = Director.basedir+"/perfmodel.txt"
	logger.addAppender( new FileAppender(new PatternLayout(Director.logPattern),logPath,false) )
	logger.setLevel(DEBUG)
}

case class L1PerformanceModel(
	modelURL:String
) extends PerformanceModel {
	
	var rconn: RConnection = null
	val latencyOverloadedThreshold = 150
	
	var timer:Long = 0
	
	connectToR
	loadModel
	
	def connectToR() {
		try { rconn = new RConnection("127.0.0.1") } catch { 
			case e:Exception => {
				logger.warn("can't connect to Rserve on localhost (run R CMD Rserve --RS-workdir <absolute path to scads/experiments/>)")
				e.printStackTrace
			}
		}
	}
	
	def loadModel() {
		rconn.parseAndEval(" source(\"../scripts/perfmodels/l1_perf_model.R\") ")
		rconn.parseAndEval(" load.libraries() ")
		if (modelURL.startsWith("http://"))
			rconn.parseAndEval(" load(url(\""+modelURL+"\")) ")
		else
			rconn.parseAndEval(" load(\""+modelURL+"\") ")
	}
	
	def sample(features:Map[String,String], nSamples:Int): List[Double] = {
		val time0 = new Date
		val rResult = rconn.parseAndEval(" rq.gp.sample(models,\""+ features("type") +"\","+features("getw")+","+features("putw")+","+nSamples+","+latencyOverloadedThreshold+") ")
		val result = try { rResult.asDoubles.toList } catch { case _ => List[Double]() }
		timer += (new Date().getTime-time0.getTime)
		result
	}
	
	def estimateLatency(features:Map[String,String], quantile:Double):Double = {
		throw new Exception("not implemented")
	}
	
	def timeInModel():Long = timer
	def resetTimer() {timer=0}
}


case class BinaryThroughputModel(
	description:String
) {
	var coefficients = scala.collection.mutable.Map[String,Double]()

	initialize

	def initialize {
		val s = description.split(":")
		
		coefficients = scala.collection.mutable.Map[String,Double]()
		var scale = scala.collection.mutable.Map[String,Double]()
		
		s(1).split(",").foreach( k =>
			if (k.startsWith("scale-"))
				scale += k.split("=")(0).split("-")(1) -> k.split("=")(1).toDouble
			else
				coefficients += k.split("=")(0) -> k.split("=")(1).toDouble
		)
	}
	
	def overloaded(getw:Double, putw:Double):Boolean = {
		coefficients("(Intercept)") + getw*coefficients("getw") + putw*coefficients("putw") > 0
	}
}

case class L1PerformanceModelWThroughput(
	modelURL:String
) extends PerformanceModel {
	var models = scala.collection.mutable.Map[String,scala.collection.mutable.Map[Double,GetPutLinearModel]]()
	
	var timer:Long = 0
	val latencyOverloadedThreshold = 150
	val latencyOverloadedQuantile = 0.99
	
	var throughputModel:BinaryThroughputModel = null
	
	initialize
	
	def initialize() {
		val source = if (modelURL.startsWith("http://")) Source.fromURL(modelURL) else Source.fromFile(modelURL)
		for (modelDescription <- source.getLines) {
			if (modelDescription.startsWith("throughput")) {
				println("creating thr model")
				throughputModel = BinaryThroughputModel(modelDescription)
			} else {
				val model = GetPutLinearModel(modelDescription)
				if (!models.contains(model.reqType)) models += model.reqType -> scala.collection.mutable.Map[Double,GetPutLinearModel]()
				models(model.reqType) += model.quantile -> model
			}
		}
	}
	
	def estimateLatency(input:Map[String,String], quantile:Double):Double = {
		val model = models(input("type"))(quantile)
		val features = model.createFeatures(Map("g"->input("getw").toDouble, "p"->input("putw").toDouble))
		model.predict(features)
	}
	
	def sample(input:Map[String,String], nSamples:Int): List[Double] = {
		val time0 = new Date
		var samples:List[Double] = null
		if (throughputModel.overloaded(input("getw").toDouble,input("putw").toDouble))
			samples = List.make(nSamples,scala.Math.POS_INF_DOUBLE)
		else if (nSamples==0)
			samples = List[Double]()
		else
			samples = sampleModels(models(input("type")), input("getw").toDouble, input("putw").toDouble, nSamples)
		
		timer += (new Date().getTime-time0.getTime)
		samples
	}

	def sampleModels(models:scala.collection.mutable.Map[Double,GetPutLinearModel], getw:Double, putw:Double, nSamples:Int):List[Double] = {
		// qv -- quantiles of the models
		val qv = models.keySet.toList.sort(_<_).toArray
		val maxq = qv.reduceLeft(Math.max(_,_))
		val minq = qv.reduceLeft(Math.min(_,_))

		// pv -- predictions of all the models
		val features = models.values.toList(0).createFeatures(Map("g"->getw,"p"->putw))
		val pv = models.values.toList.sort(_.quantile<_.quantile).map(_.predict(features)).toArray
		
		val rqs = (1 to nSamples).map(i=>Director.nextRndDouble*(maxq-minq)+minq).toList.sort(_<_)

		var qi = 0
		for (rs <- rqs) yield {
			if (rs>qv(qi+1)) qi+=1
			pv(qi) + (pv(qi+1)-pv(qi))*(rs-qv(qi))/(qv(qi+1)-qv(qi))
		}
	}
	
	def timeInModel():Long = timer
	def resetTimer() {timer=0}	
}

case class LocalL1PerformanceModel(
	modelURL:String
) extends PerformanceModel {
	
	var models = scala.collection.mutable.Map[String,scala.collection.mutable.Map[Double,GetPutLinearModel]]()
	initialize
	
	var timer:Long = 0
	val latencyOverloadedThreshold = 150
	val latencyOverloadedQuantile = 0.99
	
	def initialize() {
		val source = if (modelURL.startsWith("http://")) Source.fromURL(modelURL) else Source.fromFile(modelURL)
		for (modelDescription <- source.getLines) {
			val model = GetPutLinearModel(modelDescription)
			if (!models.contains(model.reqType)) models += model.reqType -> scala.collection.mutable.Map[Double,GetPutLinearModel]()
			models(model.reqType) += model.quantile -> model
		}
	}
	
	def estimateLatency(input:Map[String,String], quantile:Double):Double = {
		val model = models(input("type"))(quantile)
		val features = model.createFeatures(Map("g"->input("getw").toDouble, "p"->input("putw").toDouble))
		model.predict(features)
	}
	
	def sample(input:Map[String,String], nSamples:Int): List[Double] = {
		val time0 = new Date
		var samples:List[Double] = null
		if (overloaded(input("getw").toDouble,input("putw").toDouble,latencyOverloadedQuantile,latencyOverloadedThreshold))
			samples = List.make(nSamples,scala.Math.POS_INF_DOUBLE)
		else if (nSamples==0)
			samples = List[Double]()
		else
			samples = sampleModels(models(input("type")), input("getw").toDouble, input("putw").toDouble, nSamples)
		
		timer += (new Date().getTime-time0.getTime)
		samples
	}

	def sampleModels(models:scala.collection.mutable.Map[Double,GetPutLinearModel], getw:Double, putw:Double, nSamples:Int):List[Double] = {
		// qv -- quantiles of the models
		val qv = models.keySet.toList.sort(_<_).toArray
		val maxq = qv.reduceLeft(Math.max(_,_))
		val minq = qv.reduceLeft(Math.min(_,_))

		// pv -- predictions of all the models
		val features = models.values.toList(0).createFeatures(Map("g"->getw,"p"->putw))
		val pv = models.values.toList.sort(_.quantile<_.quantile).map(_.predict(features)).toArray
		
		val rqs = (1 to nSamples).map(i=>Director.nextRndDouble*(maxq-minq)+minq).toList.sort(_<_)

		var qi = 0
		for (rs <- rqs) yield {
			if (rs>qv(qi+1)) qi+=1
			pv(qi) + (pv(qi+1)-pv(qi))*(rs-qv(qi))/(qv(qi+1)-qv(qi))
		}
	}
	
	def overloaded(getw:Double, putw:Double, quantile:Double, overloadThreshold:Double):Boolean = {
		val features = models("get")(0.9).createFeatures(Map("g"->getw,"p"->putw))
		models("get")(quantile).predict(features)>overloadThreshold || models("put")(quantile).predict(features)>overloadThreshold
	}

	def timeInModel():Long = timer
	def resetTimer() {timer=0}
}


case class GetPutLinearModel(
	modelDescription:String
) {
	var coefficients:scala.collection.mutable.Map[String,Double] = null
	var scale:scala.collection.mutable.Map[String,Double] = null

	var name:String = ""
	var reqType:String = ""
	var quantile:Double = -1
	
	initModel
	
	def initModel() {
		val s = modelDescription.split(":")
		val s0 = s(0).split("-")
		name = s(0)
		reqType = s0(0)
		quantile = s0(1).toDouble/100.0
		
		coefficients = scala.collection.mutable.Map[String,Double]()
		scale = scala.collection.mutable.Map[String,Double]()
		
		s(1).split(",").foreach( k =>
			if (k.startsWith("scale-"))
				scale += k.split("=")(0).split("-")(1) -> k.split("=")(1).toDouble
			else
				coefficients += k.split("=")(0) -> k.split("=")(1).toDouble
		)
	}
	
	def predict(features:Map[String,Double]):Double = features.map( f => f._2 * coefficients(f._1) ).reduceLeft(_+_)
	
	def createFeatures(input:Map[String,Double]):Map[String,Double] = {
		val features = scala.collection.mutable.Map[String,Double]()
		val g = input("g") * scale("g")
		val p = input("p") * scale("p")
		
		val fs = coefficients.keySet.toList.map( f =>
			f -> (f match {
				case "(Intercept)" => 1.0
				case "g" => g
				case "p" => p
				case "g2" => Math.pow(g,2)
				case "g3" => Math.pow(g,3)
				case "g4" => Math.pow(g,4)
				case "expg8" => Math.pow( Math.exp(g), 8 )
				case "expg15" => Math.pow( Math.exp(g), 15 )
				case "p2" => Math.pow(p,2)
				case "p3" => Math.pow(p,3)
				case "p4" => Math.pow(p,4)
				case "expp8" => Math.pow( Math.exp(p), 8 )
				case "expp15" => Math.pow( Math.exp(p), 15 )
				case "g2_p" => g*g*p;
				case "g3_p" => g*g*g*p;
				case "g4_p" => g*g*g*g*p;
				case "expg8_p" => Math.pow( Math.exp(g), 8 )*p;
				case "expg15_p" => Math.pow( Math.exp(g), 15 )*p;
				case "p2_g" => Math.pow(p,2)*g;
				case "p3_g" => Math.pow(p,3)*g;
				case "p4_g" => Math.pow(p,4)*g;
				case "expp8_g" => Math.pow( Math.exp(p), 8 )*g;
				case "expp15_g" => Math.pow( Math.exp(p), 15 )*g;
				case "g2_p2" => Math.pow(g,2)*Math.pow(p,2)
				case "g3_p2" => Math.pow(g,3)*Math.pow(p,2)
				case "g4_p2" => Math.pow(g,4)*Math.pow(p,2)
				case "expg8_p2" => Math.pow( Math.exp(g), 8 )*Math.pow(p,2)
				case "expg15_p2" => Math.pow( Math.exp(g), 15 )*Math.pow(p,2)
				case "p3_g2" => Math.pow(p,3)*Math.pow(g,2)
				case "p4_g2" => Math.pow(p,4)*Math.pow(g,2)
				case "expp8_g2" => Math.pow( Math.exp(p), 8 )*Math.pow(g,2)
				case "expp15_g2" => Math.pow( Math.exp(p), 15 )*Math.pow(g,2)				
			})
		)
		Map( fs:_* )
	}
}
