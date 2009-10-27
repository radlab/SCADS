package edu.berkeley.cs.scads.placement

import org.apache.log4j.Logger
import org.apache.log4j.BasicConfigurator

object SimpleDataPlacementApp {
	def main(args:Array[String]) = {
		BasicConfigurator.configure()
        val port: Int= args.length match {
            case 0 => 8000
            case _ => Integer.parseInt(args(0)) 
        }
		val dps = new RunnableDataPlacementServer(port)
        println("Started...")
	}
}
