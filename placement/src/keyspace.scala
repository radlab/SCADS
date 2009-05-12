import scala.util.Sorting

class NotContiguousException extends Exception
class NonCoveredRangeException extends Exception
class NoNodeResponsibleException extends Exception
class NullKeyLookupException extends Exception

object KeyRange {
	val EmptyRange = new KeyRange("", "")
}

case class KeyRange(start: String, end: String) {
	if(start != null && end != null)
		assert(start <= end,"keyspace.scala: "+start +" !<= "+end)

	def + (that: KeyRange): KeyRange = {
		if(this == KeyRange.EmptyRange)
			that
		else if(that == KeyRange.EmptyRange)
			this
		else if((this.start == null || that.start == null) && (this.end == null || that.end == null))
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
		else if ( (this.start == null && this.end >= that.start) || (that.start == null && that.end >= this.start))
			new KeyRange(coalesce(this.start, that.start), min(this.end, that.end))
		else if ( (this.end == null && this.start < that.end) || (that.end == null && that.start < this.end) )
			new KeyRange(max(this.start, that.start), coalesce(this.end, that.end))			
		else if (this.end != null && that.end!=null && this.start != null && that.start != null) {
			if( (this.end >= that.start) && (this.end < that.end) && (this.start < that.start) )
				new KeyRange(that.start, this.end)
			else if( (that.end >= this.start) && (that.end < this.end) && (that.start < this.start) )
				new KeyRange(this.start, that.end)
			else if(this.start >= that.start && this.end <= that.end)
				this
			else if(that.start >= this.start && that.end <= this.end)
				that
			else
				KeyRange.EmptyRange
		 }
		else
			KeyRange.EmptyRange
	}

	def includes(key: String):Boolean = {
		if (key==null) throw new NullKeyLookupException
		else {
		if ( this.start==null) {
			if ( this.end==null ) true
			else if ( key< this.end ) true
			else false
		}
		else if (this.end==null) {
			if ( key>=this.start ) true
			else false
		}
		else 
			key >= this.start && key < this.end	
		}
	}

	private def min(a: String, b: String) = if(a < b) a else b
	private def max(a: String, b: String) = if(a > b) a else b
	private def coalesce(a: String, b:String) = if(a != null) a else b
}

abstract class KeySpace {
	def assign(node: StorageNode, range: KeyRange)
	def remove(node: StorageNode)

	def lookup(node: StorageNode): KeyRange
	def lookup(key: String):List[StorageNode]
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
		space.get(node).getOrElse(KeyRange("", ""))

	def lookup(key: String):List[StorageNode] =
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