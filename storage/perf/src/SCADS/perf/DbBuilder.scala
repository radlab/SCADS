package SCADS.perf;

object BuildDB extends SequentialWriter(1024) with ClosedRunner with SingleConnection with ReportToCSVFile {
	val host = "localhost"
	val port = 9000
	
	def main(args: Array[String]) = {
		exec(1000)
	}
}