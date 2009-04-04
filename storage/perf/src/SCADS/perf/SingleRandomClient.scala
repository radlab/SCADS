package SCADS.perf;

object RandomConcurrencyTest  {
	
	class RandomClient(keySpace: Int, rCount: Int) extends RandomReader(keySpace) with ClosedRunner with SingleConnection with ReportToCSVFile with Runnable {
		val host = "localhost"
		val port = 9000
		val reqCount= rCount
		otherData += ("engine" -> "bdb", "instance_type" -> "small", "version" -> "6bb0cf346abce5e6063070a6c5e59af1a6a60f87")
		
		def run() = {
			exec(rCount)
		}
	}
	
	def main(args: Array[String]) = {
		(1024 to (1024*1024) by (1024*100)).foreach( (maxKey) =>
			(1 to 15).foreach( (numThreads) => {
				val threads = (1 to numThreads).map( (id) => new Thread(new RandomClient(maxKey, 500000/numThreads)))
				threads.foreach((thread) => thread.start())
				threads.foreach((thread) => thread.join())
			}
			)
		)
	}
}