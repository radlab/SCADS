package scads.director

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
	
	val logger = Logger.getLogger("scads.perfmodel")
	private val logPath = Director.basedir+"/perfmodel.txt"
	logger.addAppender( new FileAppender(new PatternLayout(Director.logPattern),logPath,false) )
	logger.setLevel(DEBUG)
}

case class L1PerformanceModel(
	modelURL:String
) extends PerformanceModel {
	
	var rconn: RConnection = null
	val latencyThreshold = 150
	
	connectToR
	loadModel
	
	def connectToR() {
		try { rconn = new RConnection("127.0.0.1") } catch { 
			case e:Exception => {
				logger.warn("can't connect to Rserve on localhost (run R CMD Rserve --RS-workdir <absolute path to ???>)")
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
		rconn.parseAndEval(" rq.gp.sample(models,\""+ features("type") +"\","+features("getw")+","+features("putw")+","+nSamples+","+latencyThreshold+") ").asDoubles.toList
/*		null*/
	}
}