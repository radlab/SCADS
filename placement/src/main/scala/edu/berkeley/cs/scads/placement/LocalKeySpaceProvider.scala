trait LocalKeySpaceProvider extends KeySpaceProvider {
	var ns_map = new HashMap[String,KeySpace]

	override def getKeySpace(ns: String): KeySpace = { ns_map(ns) }
	override def refreshKeySpace() = {}

	def add_namespace(ns: String): Boolean = {
		this.add_namespace(ns,null)
	}
	def add_namespace(ns: String, ks: SimpleKeySpace): Boolean = {
		ns_map.update(ns,ks)
		ns_map.contains(ns)
	}

	def getMap: HashMap[String,KeySpace] = ns_map
}
