package edu.berkeley.cs.scads.director
import edu.berkeley.cs.avro.runtime._

import net.lag.logging.Logger


@serializable
case class WorkloadFeatures(
	getRate: Double,
	putRate: Double,
	getsetRate: Double
) extends Ordered[WorkloadFeatures]{

	override def toString():String = {
		"WorkloadFeatures("+"%.0f".format(getRate)+","+"%.0f".format(putRate)+","+"%.0f".format(getsetRate)+")"
	}

	def add(that:WorkloadFeatures):WorkloadFeatures = {
		WorkloadFeatures(this.getRate+that.getRate,this.putRate+that.putRate,this.getsetRate+that.getsetRate)
	}

	// if this workload is running against n machines, each machine get 1/n of the gets
	def splitGets(n:Int):WorkloadFeatures = {
		WorkloadFeatures(this.getRate/n,this.putRate,this.getsetRate)
	}
	/**
	* Divde the gets amongst replicas, and indicate the amount of allowed puts (in range 0.0-1.0)
	* Workload increases are applied before dividing amongst replicas or restricting puts
	*/
	def restrictAndSplit(replicas:Int, allowed_puts:Double, percentIncrease:Double):WorkloadFeatures = {
		assert(allowed_puts <= 1.0 && allowed_puts >= 0.0, "Amount of allowed puts must be in range [0.0 - 1.0]")
		WorkloadFeatures((this.getRate*(1.0+percentIncrease))/replicas,(this.putRate*(1.0+percentIncrease))*allowed_puts,this.getsetRate*(1.0+percentIncrease))
	}
	/**
	* Divde the gets amongst replicas, assumming quorum servers get all the workload for each request
	* and indicate the amount of allowed puts (in range 0.0-1.0)
	* Workload increases are applied before dividing amongst replicas or restricting puts
	*/
	def restrictAndSplitQuorum(quorum:Int,replicas:Int, allowed_puts:Double, percentIncrease:Double):WorkloadFeatures = {
		assert(allowed_puts <= 1.0 && allowed_puts >= 0.0, "Amount of allowed puts must be in range [0.0 - 1.0]")
		val workShare = replicas.toDouble/quorum.toDouble
		//Policy.logger.info("Have quorum = "+quorum+", replicas = " +replicas+", share: "+workShare+". old gets = "+this.getRate+", new gets = "+(this.getRate*(1.0+percentIncrease))/workShare)
		WorkloadFeatures((this.getRate*(1.0+percentIncrease))/workShare,(this.putRate*(1.0+percentIncrease))*allowed_puts,this.getsetRate*(1.0+percentIncrease))
	}
	def compare(that: WorkloadFeatures):Int = {
		val thistotal = this.sum
		val thattotal = that.sum
		if (thistotal < thattotal) -1
		else if (thistotal == thattotal) 0
		else 1
	}
	def sum:Double = (if (!this.getRate.isNaN) {this.getRate} else 0.0) +
					 (if (!this.putRate.isNaN) {this.putRate} else 0.0) +
					 (if (!this.getsetRate.isNaN) {this.getsetRate} else {0.0})

	def max(that:WorkloadFeatures):WorkloadFeatures = {
		WorkloadFeatures(
			scala.math.max(this.getRate,that.getRate),
			scala.math.max(this.putRate,that.putRate),
			scala.math.max(this.getsetRate,that.getsetRate) )
	}

	def + (that:WorkloadFeatures):WorkloadFeatures = {
		WorkloadFeatures(this.getRate+that.getRate,this.putRate+that.putRate,this.getsetRate+that.getsetRate)
	}
	def - (that:WorkloadFeatures):WorkloadFeatures = {
/*		WorkloadFeatures(scala.math.abs(this.getRate-that.getRate),scala.math.abs(this.putRate-that.putRate),scala.math.abs(this.getsetRate-that.getsetRate))*/
		WorkloadFeatures(this.getRate-that.getRate,this.putRate-that.putRate,this.getsetRate-that.getsetRate)
	}
	def * (multiplier:Double):WorkloadFeatures = {
		WorkloadFeatures(this.getRate*multiplier,this.putRate*multiplier,this.getsetRate*multiplier)
	}
}

/**
* Represents the histogram of keys in workload
*/
@serializable
case class WorkloadHistogram (
	val rangeStats: Map[Option[org.apache.avro.generic.GenericData.Record],WorkloadFeatures]
) extends Ordered[WorkloadHistogram] {
	def divide(replicas:Int, allowed_puts:Double):WorkloadHistogram = {
		new WorkloadHistogram(
			Map[Option[org.apache.avro.generic.GenericData.Record],WorkloadFeatures](rangeStats.toList map {entry => (entry._1, entry._2.restrictAndSplit(replicas,allowed_puts,0.0)) } : _*)
		)
	}
	override def toString():String = rangeStats.toList.mkString("\n")
/*
	def multiplyKeys(factor:Double):WorkloadHistogram = {
		WorkloadHistogram( Map( rangeStats.toList.map( rs => DirectorKeyRange((rs._1.minKey*factor).toInt,(rs._1.maxKey*factor).toInt) -> rs._2 ) :_* ) )
	}

	def multiplyWorkload(factor:Double):WorkloadHistogram = {
		WorkloadHistogram( Map( rangeStats.toList.map( rs => (rs._1) -> (rs._2*factor) ) :_* ) )
	}
*/
	def compare(that:WorkloadHistogram):Int = { // do bin-wise comparison of workloadfeatures, return summation
		this.rangeStats.toList.map(entry=>entry._2.compare( that.rangeStats(entry._1) )).reduceLeft(_+_)
	}
	/**
	* Split this workload into ranges such that the workload is more or less balanced between them
	*/
	/*
	def split(pieces:Int):List[List[DirectorKeyRange]] = {
		val splits = new scala.collection.mutable.ListBuffer[List[DirectorKeyRange]]()
		val targetworkload = this.rangeStats.values.reduceLeft(_+_).sum.toInt/pieces // the workload each split should aim for
		var split = new scala.collection.mutable.ListBuffer[DirectorKeyRange]()
		var splitworkload = 0
		this.rangeStats.keys.toList.sort(_.minKey < _.minKey).foreach((range)=>{
			if (splitworkload < targetworkload) { split += range; splitworkload += this.rangeStats(range).sum.toInt }
			else { splits += split.toList; split = new scala.collection.mutable.ListBuffer[DirectorKeyRange](); splitworkload = 0 }
		})
		if (!split.isEmpty) { splits += split.toList } // add the last one split, if necessary
		splits.toList
	}
*/
	def doHysteresis(that:WorkloadHistogram, alphaUp:Double, alphaDown:Double):WorkloadHistogram = {
		new WorkloadHistogram(
			Map( this.rangeStats.keys.toList.map( range => {
					val alpha = if (this.rangeStats(range).sum > that.rangeStats(range).sum) alphaDown else alphaUp;
					range -> (this.rangeStats(range) + (that.rangeStats(range)-this.rangeStats(range))*alpha) } ) :_* )
		)
	}

	def max(that:WorkloadHistogram):WorkloadHistogram = {
		val ranges = this.rangeStats.keys.toList
		WorkloadHistogram( Map( ranges.map( r => r -> this.rangeStats(r).max(that.rangeStats(r)) ) :_* ) )
	}

	def + (that:WorkloadHistogram):WorkloadHistogram = {
		new WorkloadHistogram(
			Map[Option[org.apache.avro.generic.GenericData.Record],WorkloadFeatures]( this.rangeStats.toList map {entry=>(entry._1, entry._2+that.rangeStats(entry._1))} : _*)
		)
	}
	def - (that:WorkloadHistogram):WorkloadHistogram = {
		new WorkloadHistogram(
			Map[Option[org.apache.avro.generic.GenericData.Record],WorkloadFeatures]( this.rangeStats.toList map {entry=>(entry._1, entry._2-that.rangeStats(entry._1))} : _*)
		)
	}
	def * (multiplier:Double):WorkloadHistogram = {
		new WorkloadHistogram(
			Map[Option[org.apache.avro.generic.GenericData.Record],WorkloadFeatures]( this.rangeStats.toList map {entry=>(entry._1, entry._2*multiplier)} : _*)
		)
	}

	def totalRate:Double = rangeStats.map(_._2.sum).reduceLeft(_+_)
	def totalGetRate:Double = rangeStats.map(_._2.getRate).reduceLeft(_+_)
	def totalPutRate:Double = rangeStats.map(_._2.putRate).reduceLeft(_+_)

	def toShortString():String = {
		val getRate = rangeStats.map(_._2.getRate).reduceLeft(_+_)
		val putRate = rangeStats.map(_._2.putRate).reduceLeft(_+_)
		"WorkloadHistogram: (total rates) get="+"%.2f".format(getRate)+"r/s, put="+"%.2f".format(putRate)+"r/s"
	}

	def toCSVString():String = {
		"startkey,getRate,putRate,getsetRate\n"+
		rangeStats.keySet.toList//.sort(_<_)
			.map( r=>r+","+rangeStats(r).getRate+","+rangeStats(r).putRate+","+rangeStats(r).getsetRate )
			.mkString("\n")
	}

	def toCSVHeader():String = rangeStats.keySet.toList/*.sort(_<_)*/.mkString(",")
	def toCSVWorkload():String = rangeStats.keySet.toList/*.sort(_ < _)*/.mkString(",")
}
