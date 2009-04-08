package SCADS.perf;

import java.io.BufferedWriter
import java.io.FileWriter

trait Runner {
	var otherData: Map[String, String] = Map("client" -> java.net.InetAddress.getLocalHost().getHostName())

	def useConnection(): Map[String, String]
	def timeRequest(): Map[String, String] = {
		val start = System.currentTimeMillis()
		var ret = useConnection()
		val end = System.currentTimeMillis()
		
		ret + ("start_time" -> ("" + start), "end_time" -> ("" + end)) ++ otherData
	}
	
	def report(stats: Map[String, String])
}

trait ReportToCSVFile {
	val file = new FileWriter("perf_data" + System.currentTimeMillis().toString() + ".csv")
	var keys: Seq[String] = null
	
	def report(stats: Map[String, String]){
		if(keys == null) {
			keys = stats.keys.collect
			file.write("#" + keys.mkString("", ",", "") + "\n")
		}
		file.write(keys.map((k) => stats(k)).mkString("", ",", "") + "\n")
		file.flush()
	}
}

trait ClosedRunner extends Runner {
	def exec(count: Int) = {
		val gcInterval = 1000
		for(i <- 1 to count) {
			if (i % 1000 == 0) System.gc()
			report(timeRequest() + ("runner" -> "closed", "gc_interval" -> gcInterval.toString()))
		}
	}
}