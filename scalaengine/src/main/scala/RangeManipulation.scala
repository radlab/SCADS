package edu.berkeley.cs.scads.storage

import edu.berkeley.cs.scads.comm._
import edu.berkeley.cs.scads.test._
import edu.berkeley.cs.scads.comm.Conversions._
import org.apache.avro.specific.SpecificRecordBase

class NotContiguousException extends Exception
class NonCoveredRangeException extends Exception
class NoNodeResponsibleException extends Exception
class NullKeyLookupException extends Exception

/**
* These methods assume non-null entries in the ranges
*/
object RangeManipulation {
	val EmptyRange = (new IntRec,new IntRec)
	val FakeRange = (new IntRec,new IntRec)
	FakeRange._1.f1= -1; FakeRange._2.f1= -1;

	/**
	* Return union of the two ranges
	*/
	def union(one:(SpecificRecordBase,SpecificRecordBase),two:(SpecificRecordBase,SpecificRecordBase)):(SpecificRecordBase,SpecificRecordBase) = {

		if (one == EmptyRange) two
		else if (two == EmptyRange) one

		else if (one._2.compareTo(two._1) >= 0 && one._2.compareTo(two._2) < 0 && one._1.compareTo(two._1) < 0)
			(one._1, two._2)
		else if (two._2.compareTo(one._1) >= 0 && two._2.compareTo(one._2) < 0 && two._1.compareTo(one._1) < 0)
			(two._1, one._2)
		else if (one._1.compareTo(two._1) >= 0 && one._2.compareTo(two._2) <= 0)
			two
		else if (two._1.compareTo(one._1) >= 0 && two._2.compareTo(one._2) <= 0)
			one
		else 
			throw new NotContiguousException
	}
	
	/**
	* Substract two from one (i.e. one-two)
	*/
	def difference(one:(SpecificRecordBase,SpecificRecordBase),two:(SpecificRecordBase,SpecificRecordBase)):(SpecificRecordBase,SpecificRecordBase) = {
		
		if (one._2.compareTo(two._1) > 0 && one._2.compareTo(two._2) <= 0 && one._1.compareTo(two._1) < 0)
			(one._1, two._1)
		else if (one._1.compareTo(two._1) >= 0 && one._1.compareTo(two._2) < 0 && one._2.compareTo(two._2) > 0)
			(two._2, one._2)
		else if (one._2.compareTo(two._1) < 0 || one._1.compareTo(two._2) > 0)
			one
		else if (one._1.compareTo(two._1) == 0 || one._2.compareTo(two._2) == 0)
			EmptyRange
		else
			throw new NotContiguousException
	}
	
	/**
	* Return the range which is intersection of the two given
	*/
	def intersection(one:(SpecificRecordBase,SpecificRecordBase),two:(SpecificRecordBase,SpecificRecordBase)):(SpecificRecordBase,SpecificRecordBase) = {
	
		if (one._2.compareTo(two._1) > 0 && one._2.compareTo(two._2) < 0 && one._1.compareTo(two._1) < 0)
			(two._1, one._2)
		else if (two._2.compareTo(one._1) > 0 && two._2.compareTo(one._2) < 0 && two._1.compareTo(one._1) < 0)
			(one._1, two._2)
		else if (one._1.compareTo(two._1) >= 0 && one._2.compareTo(two._2) <= 0)
			one
		else if (two._1.compareTo(one._1) >= 0 && two._2.compareTo(one._2) <= 0)
			two
		else
			EmptyRange
	}
	/**
	* Return boolean if two ranges are adjacent (but not overlapping)
	*/
	def areAdjacent(one:(SpecificRecordBase,SpecificRecordBase),two:(SpecificRecordBase,SpecificRecordBase)):Boolean = {
		if ((one._1.equals(two._2)) || (two._1.equals(one._2))) true
		else false
	}
	
	/**
	* Boolean indicating if the key falls within the range, assuming range is [start,end)
	*/
	def includesKey(key:SpecificRecordBase,one:(SpecificRecordBase,SpecificRecordBase)):Boolean = {
		key.compareTo(one._1) >= 0 && key.compareTo(one._2) < 0
	}
	
	/**
	* Boolean indicating if the key falls within one of the ranges
	*/
	def includesKey(key:SpecificRecordBase,ranges:List[(SpecificRecordBase,SpecificRecordBase)]):Boolean = {
		var found = false
		ranges.foreach((range)=> { if(includesKey(key,range)) {found=true} })
		found	
	}
	
	/**
	* Returns boolean indicating if one of the ranges in haystack contains the target range, needle
	*/
	def contains(needle:(SpecificRecordBase,SpecificRecordBase), haystack:List[(SpecificRecordBase,SpecificRecordBase)]):Boolean = {
		var found = false
		
		haystack.foreach(range=>{
			try { if (union(range,needle).equals(range)) found = true }
			catch { case e:NotContiguousException => /* do nothing */ }
		})
		
		found
	}
	
	/**
	* Add a range to a list of ranges. If the new range overlaps one or more of the existing ranges, they will coalesce
	* Assumes list of ranges does not include overlapping ranges
	*/
	def addRange(new_range:(SpecificRecordBase,SpecificRecordBase), ranges:List[(SpecificRecordBase,SpecificRecordBase)]):List[(SpecificRecordBase,SpecificRecordBase)] = {
		val sorted_ranges = sortRanges(ranges)
		var result = new scala.collection.mutable.ListBuffer[(SpecificRecordBase,SpecificRecordBase)]()
		var to_add = new_range
		
		sorted_ranges.foreach(range=>{
			if (areAdjacent(range,to_add) || (intersection(range, to_add) != EmptyRange)) to_add = union(range, to_add) // they overlap, combine them
			else if (to_add._2.compareTo(range._1) <= 0 || range._2.compareTo(to_add._1) < 0)
				result += range // don't overlap and existing range smaller/larger than what we're adding, add existing back to list
		})
		result += to_add
		result.toList
	}
	/**
	* Return the list of ranges after removing the given range.
	* Assumes list of ranges does not include overlapping ranges
	*/
	def removeRange(new_range:(SpecificRecordBase,SpecificRecordBase), ranges:List[(SpecificRecordBase,SpecificRecordBase)]):List[(SpecificRecordBase,SpecificRecordBase)] = {
		val sorted_ranges = sortRanges(ranges)
		var result = new scala.collection.mutable.ListBuffer[(SpecificRecordBase,SpecificRecordBase)]()
		var to_rem = new_range
		
		sorted_ranges.foreach(range=>{
			if ( range._2.compareTo(to_rem._1) <= 0 || range._1.compareTo(to_rem._2) >= 0 )
				result += range
			else if (union(range,to_rem).equals(to_rem)) {} 	// range inside to_rem (or equal), don't do anything
			else if (union(range,to_rem).equals(range)) {			// to_rem falls completely inside range, split range into [at most] two
				if (!range._1.equals(to_rem._1)) result += Tuple2(range._1,to_rem._1)
				if (!to_rem._2.equals(range._2)) result += Tuple2(to_rem._2,range._2)
			}
			else if (includesKey(to_rem._1,range) 	// if to_rem.start falls within range, add first part of range back to list
				|| includesKey(to_rem._2,range))			// if to_rem.end falls within range, add last part of range back to list
				result += difference(range,to_rem)
		})
		result.toList
	}
	
	def sortRanges(ranges:List[(SpecificRecordBase,SpecificRecordBase)]):List[(SpecificRecordBase,SpecificRecordBase)] = {
		ranges.sort((one,two)=> { 
			val comp = if (!one._1.equals(two._1)) one._1.compareTo(two._1) else one._2.compareTo(two._2)
			if (comp < 0) true else false 
		})
	}
}
