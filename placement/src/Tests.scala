object SimpleSetup {

	def main(args: Array[String]) = {
		val cl = new ClientLibraryServer(7911,4000)
		cl.start
	}

}