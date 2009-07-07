package edu.berkeley.cs.scads.keys

class NotContiguousException extends Exception
class NonCoveredRangeException extends Exception
class NoNodeResponsibleException extends Exception
class NullKeyLookupException extends Exception

object KeyRange {
	val EmptyRange = new KeyRange(new StringKey(""), new StringKey(""))
}

case class KeyRange(start: Key, end: Key) {
	if(start != null && end != null)
		assert(start <= end,"keyspace.scala: "+start +" !<= "+end)

	def + (that: KeyRange): KeyRange = {
		if(this == KeyRange.EmptyRange)
			that
		else if(that == KeyRange.EmptyRange)
			this
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

	def includes(key: Key):Boolean = {
		key >= this.start && key < this.end
	}

	private def min(a: Key, b: Key) = if(a < b) a else b
	private def max(a: Key, b: Key) = if(a > b) a else b
	private def coalesce(a: Key, b:Key) = if(a != null) a else b
}

