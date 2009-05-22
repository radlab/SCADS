object CSVWriter {
	val files = new scala.collection.immutable.HashMap[java.io.File, java.io.FileWriter]
	val timestamp = System.currentTimeMillis().toString
}

trait CSVWriter {
	val resultPrefix:String
	val hostname = java.net.InetAddress.getLocalHost().getHostName()
	def output(data: Map[String, String]) {
		val keys = data.keys.toList.sort(_ < _)
		val keyHash = join(keys, "").hashCode
		val resultFile = new java.io.File(resultPrefix, "results." + hostname + "." + keyHash + "." + CSVWriter.timestamp + ".csv")
		val writer: java.io.FileWriter =
			if(CSVWriter.files contains resultFile)
				CSVWriter.files(resultFile)
			else {
				val newfile = !resultFile.exists()
				val ret = new java.io.FileWriter(resultFile, true)

				if(newfile)
					ret.write(join(keys, ",") + "\n")
				ret
			}

		val result = join(keys.map((key) => data(key)), ",") + "\n"
		writer.write(result)
		writer.flush()
	}

	private def join(list: List[String], seperator: String) = list.reduceLeft((k1: String, k2: String) => k1 + seperator + k2)
}

case class RandomReader(testData: Map[String, String], server: String, maxKey: Int, requests: Int) extends Runnable with CSVWriter {
	case class SCADSClient(host: String, port: Int) extends ROWAClientLibrary with RemoteKeySpaceProvider
	val client = new SCADSClient(server, 8000)
	val resultPrefix = "/mnt"
	
	def run() {
		var result = Map("data_placement_server" -> server, "max_key" -> maxKey.toString, "requests" -> requests.toString, "client_machine" -> java.net.InetAddress.getLocalHost().getHostName(), "client_threat" -> Thread.currentThread().getName()) ++ testData
		val keyFormat = new java.text.DecimalFormat("000000000000000")
		val rand = new java.util.Random
		def getKey(key: Int) = keyFormat.format(key)

		result +=  ("start_time" -> System.currentTimeMillis().toString)
		for(i <- 1 to requests) {
			val key = getKey(rand.nextInt(maxKey))
			client.get("perfTest", key)
		}
		result +=  ("end_time" -> System.currentTimeMillis().toString)
		
		println("test complete")
		output(result)
	}
}