object SimpleSetup {

	def main(args: Array[String]) = {
		val cl = new ClientLibraryServer(7911)
		cl.start
	}

}