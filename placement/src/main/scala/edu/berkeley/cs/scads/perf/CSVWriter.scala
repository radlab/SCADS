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