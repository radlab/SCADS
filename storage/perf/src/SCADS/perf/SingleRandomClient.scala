package SCADS.perf;

object RandomConcurrencyTest  {
	
	class RandomClient(keySpace: Int, rCount: Int) extends RandomReader(keySpace) with ClosedRunner with SingleConnection with ReportToCSVFile with Runnable {
		val host = "localhost"
		val port = 9000
		val reqCount= rCount
		otherData += ("engine" -> "bdb", "instance_type" -> "small", "version" -> "6bb0cf346abce5e6063070a6c5e59af1a6a60f87")

		def run() = {
			otherData += ("test_id" -> ("proc" + getPid() + Thread.currentThread().getName()))
			exec(rCount)
		}

		//Super gross, I hate you java
		private def getPid():String = {
			var bo = new Array[Byte](20)
			val cmd = Array("bash", "-c", "echo $PPID")
			val p = Runtime.getRuntime().exec(cmd)
			p.getInputStream().read(bo)
			new String(bo).trim()
		}
	}

	def main(args: Array[String]) = {
		for(maxKey <- (1024 to (1024 * 1024) by (1024*100))) {
			for(numThreads <- (1 to 15)) {
				println("Execing " + maxKey + " "+ numThreads)
				val threads = (1 to numThreads).toList.map((id) => {new Thread(new RandomClient(maxKey, 500000/numThreads), "thread"+id)})

				threads.foreach((thread) => thread.start())
				for(t <- threads) t.join()
			}
		}
	}
}

