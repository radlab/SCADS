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
	
	var keepPlotting = false
	
	def initialize(dir:String) { 
		connectToR
		this.dir = dir
		(new File(dir)).mkdirs()
		(new File(dir+"/past_plots/")).mkdirs()

		logger = Logger.getLogger("scads.plotting")
		val logPath = Director.basedir+"/plotting.txt"
		logger.removeAllAppenders
		logger.addAppender( new FileAppender(new PatternLayout(Director.logPattern),logPath,false) )
		logger.setLevel(DEBUG)
	}
	
	def connectToR() {
		try { 
			rconn = new RConnection("127.0.0.1") 
			rconn.parseAndEval(" source(\"../scripts/plotting/scads_plots.R\") ")
		} catch { 
			case e:Exception => {
				logger.warn("can't connect to Rserve on localhost (run R CMD Rserve --RS-workdir <absolute path to scads/experiments/>)")
				e.printStackTrace
			}
		}		
	}
	
	def plotSCADSState(state:SCADSState, time0:Long, time1:Long, latency90pThr:Double, file:String) {
		if (rconn==null) logger.warn("don't have connection to R, can't plot")
		else {
			try {
				rconn.parseAndEval("  plot.scads.state(\""+time0+"\",\""+time1+"\",latency90p.thr="+latency90pThr+",out.file=\""+dir+"/"+file+"\")  ")
				rconn.parseAndEval(" disconnect.all() ")
			} catch {
				case e:Exception => { logger.warn("couldn't render SCADS state plot"); e.printStackTrace }
			}
		}
	}
	
	def startPlotting {
		keepPlotting = true	
		val plotter = new Plotter()
		val plotterThread = new Thread(plotter)
		plotterThread.start		
	}
	
	def stopPlotting {
		keepPlotting = false
	}
	
	case class Plotter() extends Runnable {
		def run = {
			while (keepPlotting) {
				plotSimpleDirectorAndConfigs
				Thread.sleep(1*60*1000)
			}
		}
	}
	
	def plotSimpleDirectorAndConfigs() {
		if (rconn==null) logger.warn("don't have connection to R, can't plot")
		else {
			try {
				val time = new java.util.Date().getTime				
				logger.info("plotting director.simple")
				rconn.parseAndEval("  plot.director.simple( out.file=\""+dir+"/past_plots/director_"+time+".png\")  ")
				Director.exec("cp '"+dir+"/past_plots/director_"+time+".png' '"+dir+"/director.png'")
				logger.info("done")
				
				logger.info("not plotting configs")
				//logger.info("plotting configs")
				//rconn.parseAndEval("  plot.configs( out.file=\""+dir+"/past_plots/configs_"+time+".png\")  ")
				//Director.exec("cp '"+dir+"/past_plots/configs_"+time+".png' '"+dir+"/configs.png'")
				//logger.info("done")
				//rconn.parseAndEval("  disconnect.all()  ")
			} catch {
				case e:Exception => { logger.warn("couldn't render director.simple or configs plot"); logger.warn("can't plot",e) }
			}
		}
	}
		
}
