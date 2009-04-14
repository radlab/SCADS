case class Node(host: String, port: Int)

class NotContiguousException extends Exception

object KeyRange {
	val EmptyRange = new KeyRange("", "")
}

case class KeyRange(start: String, end: String) {
	if(start != null && end != null)
		assert(start <= end)

	def + (that: KeyRange): KeyRange = {
		if((this.start == null || that.start == null) && (this.end == null || that.end == null))
			new KeyRange(null, null)
		else if (this.start == null || that.start == null)
			new KeyRange(null, max(this.end, that.end))
		else if (this.end == null || that.end == null)
			new KeyRange(min(this.start, that.start), null)
		else if((this.end >= that.start) && (this.end < that.end) && (this.start < that.start))
			new KeyRange(this.start, that.end)
		else if((that.end >= this.start) && (that.end < this.end) && (that.start < this.start))
			new KeyRange(that.start, this.end)
		else if(this.start >= that.start && this.end <= that.end)
			that
		else if(that.start >= this.start && that.end <= this.end)
			this
		else
			throw new NotContiguousException
	}

	def - (that: KeyRange): KeyRange = {
		if(this.end > that.start && this.end <= that.end && this.start < that.start)
			new KeyRange(this.start, that.start)
		else if(this.start >= that.start && this.start < that.end && this.end > that.end)
			new KeyRange(that.end, this.end)
		else if(this.end < that.start || this.start > that.end)
			this
		else if (this.start == this.start && this.end == this.end)
			KeyRange.EmptyRange
		else
			throw new NotContiguousException
	}

	def & (that: KeyRange): KeyRange = {
		if((this.start == null || that.start == null) && (this.end == null || that.end == null))
			new KeyRange(null, null)
		else if (this.start == null || that.start == null)
			new KeyRange(coalesce(this.start, that.start), min(this.end, that.end))
		else if (this.end == null || that.end == null)
			new KeyRange(max(this.start, that.start), coalesce(this.end, that.end))
		else if((this.end >= that.start) && (this.end < that.end) && (this.start < that.start))
			new KeyRange(that.start, this.end)
		else if((that.end >= this.start) && (that.end < this.end) && (that.start < this.start))
			new KeyRange(this.start, that.end)
		else if(this.start >= that.start && this.end <= that.end)
			this
		else if(that.start >= this.start && that.end <= this.end)
			that
		else
			KeyRange.EmptyRange
	}

	def includes(key: String) = key >= start && key < end

	private def min(a: String, b: String) = if(a < b) a else b
	private def max(a: String, b: String) = if(a > b) a else b
	private def coalesce(a: String, b:String) = if(a != null) a else b
}

abstract class KeySpace {
	def assign(node: Node, range: KeyRange)
	def remove(node: Node, range: KeyRange)

	def lookup(key: String):Iterator[Node]
	def lookup(range: KeyRange): Map[Node, KeyRange]
	def coverage: Iterator[KeyRange]
}

class SimpleKeySpace extends KeySpace {
	var space = Map[Node, KeyRange]()

	def assign(node: Node, range: KeyRange) =
	space = (space + (node -> range))

	def remove(node: Node, range: KeyRange) =
	space = (space - node)

	def lookup(key: String):Iterator[Node] =
	space.filter((pair) => pair._2.includes(key)).keys

	def lookup(range: KeyRange): Map[Node, KeyRange] =
	space.filter((pair) => (pair._2 & range) != KeyRange.EmptyRange)

	def coverage: Iterator[KeyRange] = space.values
}