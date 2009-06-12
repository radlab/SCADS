trait KeySpaceProvider {
	def getKeySpace(ns: String):KeySpace
	def refreshKeySpace()
}

abstract class KeySpace {
	def assign(node: StorageNode, range: KeyRange)
	def remove(node: StorageNode)

	def lookup(node: StorageNode): KeyRange
	def lookup(key: Key):List[StorageNode]
	def lookup(range: KeyRange): Map[StorageNode, KeyRange]
	def coverage: Iterator[KeyRange]
	def isCovered(desired_range: KeyRange, ranges: Set[KeyRange]): Boolean
}

@serializable
class SimpleKeySpace extends KeySpace {
	var space = Map[StorageNode, KeyRange]()

	def assign(node: StorageNode, range: KeyRange) =
		space = (space + (node -> range))

	def remove(node: StorageNode) =
		space = (space - node)

	def lookup(node: StorageNode): KeyRange =
		space.get(node).getOrElse(KeyRange.EmptyRange)

	def lookup(key: Key):List[StorageNode] =
		space.toList.filter((pair) => pair._2.includes(key)).map((pair) => pair._1)

	def lookup(range: KeyRange): Map[StorageNode, KeyRange] =
		space.filter((pair) => (pair._2 & range) != KeyRange.EmptyRange)

	def coverage: Iterator[KeyRange] = space.values

	def isCovered(desired_range: KeyRange, ranges: Set[KeyRange]): Boolean = {
		val rangesArray = ranges.toArray
		Sorting.stableSort(rangesArray,(r1:KeyRange,r2:KeyRange)=> {
			if (r1.start==null && r2.start==null) {
				if (r1.end==null && r2.end==null) true
				else if (r2.end==null) true
				else false
			}
			else if (r1.start==null) true
			else if (r2.start==null) false
			else if (r1.end == null || r2.end == null) r1.start < r2.start
			else (r1.start < r2.start) && (r1.end <= r2.end)
		})

		try {
			val firststart = rangesArray(0).start
			var span_range = KeyRange(firststart,firststart) // init with start-start range
			rangesArray.foreach(r=>	span_range += r)		// add all the ranges that we have

			if (
				(span_range.start==null && span_range.end==null) ||
				(span_range.start==null && desired_range.start == null && desired_range.end!=null && span_range.end >= desired_range.end) ||
				(desired_range.start!=null && span_range.start <= desired_range.start && span_range.end==null && desired_range.end == null)
				) true
			else if (desired_range.start==null || desired_range.end ==null) false
			else span_range.start <= desired_range.start && span_range.end >= desired_range.end
		} catch {
			case e:NotContiguousException => false
			case _ => false
		}
	}

	override def toString() =
		if(!space.isEmpty)
			"KeySpace\n==============\n"+ space.map((pair) => pair._1 + " => " + pair._2).reduceLeft((a,b) => a + "\n" + b)
		else
			"Empty"
}
