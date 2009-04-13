
case class Node(host: String, port: Int)

case class KeyRange(start: String, end: String) {
	def + (that: KeyRange): KeyRange = null
	def - (that: KeyRange): KeyRange = null
}

abstract class KeySpace {
	def add(node: Node, range: KeyRange)
	def remove(node: Node, range: KeyRange)
	
	def lookup(key: String):Set[Node]
	def lookup(range: KeyRange): Map[Node, KeyRange]
	def coverage: Set[KeyRange]
}

class InefficientKeySpace extends KeySpace {
	//TODO: Michael
}