package scads.director

import java.io._

import org.rosuda._
import org.rosuda.REngine._
import org.rosuda.REngine.REXPMismatchException;
import org.rosuda.REngine.REngineException;
import org.rosuda.REngine.Rserve.RConnection;
import org.rosuda.REngine.Rserve.RserveException;

import org.apache.log4j._
import org.apache.log4j.Level._


object Plotting {
	var rconn: RConnection = null
	var dir = ""
	
	var logger:Logger = null
	
	def initialize(dir:String) { 
		connectToR
		this.dir = dir
		(new File(dir)).mkdirs()

		logger = Logger.getLogger("scads.perfmodel")
		val logPath = Director.basedir+"/perfmodel.txt"
		logger.addAppender( new FileAppender(new PatternLayout(Director.logPattern),logPath,false) )
		logger.setLevel(DEBUG)
	}
	
	def connectToR() {
		try { rconn = new RConnection("127.0.0.1") } catch { 
			case e:Exception => {
				logger.warn("can't connect to Rserve on localhost (run R CMD Rserve --RS-workdir <absolute path to scads/experiments/>)")
				e.printStackTrace
			}
		}
		
		rconn.parseAndEval("  source(\"../scripts/plotting/scads_plots.R\" )")
	}
	
	def plotSCADSState(state:SCADSState, time0:Long, time1:Long, latency90pThr:Double, file:String) {
		rconn.parseAndEval("  plot.scads.state(\""+time0+"\",\""+time1+"\",latency90p.thr="+latency90pThr+",out.file=\""+dir+"/"+file+"\")  ")
	}
		
}
