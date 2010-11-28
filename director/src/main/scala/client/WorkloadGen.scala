package edu.berkeley.cs.scads.director

import scala.collection.mutable.ListBuffer
import edu.berkeley.cs.scads.comm._

abstract class ScadsRequest {
	def key:org.apache.avro.generic.GenericData.Record
}
case class ScadsGet(val key:org.apache.avro.generic.GenericData.Record) extends ScadsRequest
case class ScadsPut(val key:org.apache.avro.generic.GenericData.Record, val value:org.apache.avro.generic.GenericData.Record) extends ScadsRequest

@serializable
case class WorkloadIntervalDescription(
	val numberOfActiveUsers: Int,
	val duration: Int,
	val requestGenerator: SCADSRequestGenerator
)


@serializable
case class WorkloadDescription(
	val thinkTimeMean: Long,
	val workload: List[WorkloadIntervalDescription]
)
{
	def getMaxNUsers(): Int = workload.map(_.numberOfActiveUsers).reduceLeft(Math.max(_,_))
	
	def serialize(file: String) = {
        val out = new java.io.ObjectOutputStream( new java.io.FileOutputStream(file) )
        out.writeObject(this);
        out.close();
	}
	
}

object WorkloadDescription {
	var rnd = new java.util.Random
	var rnd_thread = new ThreadLocal[java.util.Random]()

	def setRndGenerator(r:java.util.Random) { rnd = r }

	
	def create(workloadProfile:WorkloadProfile, durations:List[Int], requestGenerators:List[SCADSRequestGenerator], thinkTimeMean:Long): WorkloadDescription = {
		create(workloadProfile, List.make(workloadProfile.profile.length,1.0), durations, requestGenerators, thinkTimeMean)
	}

	def create(workloadProfile:WorkloadProfile, relativeForecast:List[Double], durations:List[Int], requestGenerators:List[SCADSRequestGenerator], thinkTimeMean:Long): WorkloadDescription = {
		assert(workloadProfile.profile.length==durations.length && workloadProfile.profile.length==requestGenerators.length && workloadProfile.profile.length==relativeForecast.length,
			"workloadProfile, durations, and requestGenerators need to have the same length")
		val intervals = workloadProfile.profile.zip(durations).zip(requestGenerators).zip(relativeForecast).map( e => new WorkloadIntervalDescription(e._1._1._1,e._1._1._2,e._1._2) )
		new WorkloadDescription(thinkTimeMean,intervals)
	}

	def cat(workloads:List[WorkloadDescription]):WorkloadDescription =
		new WorkloadDescription(workloads(0).thinkTimeMean,
								List.flatten(workloads.map(_.workload)) )

	def deserialize(file: String): WorkloadDescription = {
        val fin = new java.io.ObjectInputStream( new java.io.FileInputStream(file) )
        val wd = fin.readObject().asInstanceOf[WorkloadDescription]
        fin.close
        wd
	}
}

object WorkloadGenerators {
	def flatWorkloadRates(getProb:Double, getsetProb:Double, getsetLength:Int, maxKey:Int, rate:Int, num_minutes:Int):WorkloadDescription = {
		val wProf = WorkloadProfile.getFlat(1,rate)
		val mix = new MixVector(Map("get"->getProb,"getset"->getsetProb,"put"->(1-getProb-getsetProb)))
		val reqGenerators = List( new FixedSCADSRequestGenerator(mix,UniformKeyGenerator(0,maxKey)) )
		val durations = List( num_minutes*60*1000 )
		WorkloadDescription.create(wProf,durations,reqGenerators,0)
	}
	def linearWorkloadRates(getProb:Double, getsetProb:Double, getsetLength:Int, maxKey:Int, startRate:Int, endRate:Int, num_intervals:Int, intervalDuration:Int):WorkloadDescription = {
		val wProf = WorkloadProfile.getLinear(num_intervals,startRate,endRate)
		val mix = new MixVector(Map("get"->getProb,"getset"->getsetProb,"put"->(1-getProb-getsetProb)))
		val reqGenerators = List.make(num_intervals, new FixedSCADSRequestGenerator(mix,UniformKeyGenerator(0,maxKey)))
		val durations = List.make(num_intervals,intervalDuration)
		WorkloadDescription.create(wProf,durations,reqGenerators,0)
	}
} // 3000 -> 14000 in one hour => 10 sec intervals, 360 intervals

@serializable
class MixVector(
	_mix: Map[String,Double]
) {
	val mix = normalize(_mix)

	private def sum(m:Map[String,Double]): Double = m.values.reduceLeft(_+_)
	private def normalize(m:Map[String,Double]): Map[String,Double] = m.transform((k,v)=>v/sum(m))
	def getMix: Map[String,Double] = mix

	def transition(that:MixVector,c:Double): MixVector = {
		new MixVector( mix.transform( (k,v) => t(v,that.mix(k),c) ) )
	}
	private def t(a:Double, b:Double, r:Double): Double = a + r*(b-a)
	override def toString() = mix.keySet.toList.sort(_<_).map(k=>k+"->"+mix(k)).mkString("m[",",","]")

	def sampleRequestType(): String = {
		val r = WorkloadDescription.rnd.nextDouble()
		var agg:Double = 0

		var reqType = ""
		for (req <- mix.keySet) {
			agg += mix(req)
			if (agg>=r && reqType=="") reqType = req
		}
		reqType
	}
}

object WorkloadProfile {
	def getFlat(nintervals:Int, nusers:Int): WorkloadProfile = {
		new WorkloadProfile( List.make(nintervals,nusers) )
	}

	def getLinear(nintervals:Int, nusersStart:Int, nusersEnd:Int): WorkloadProfile = {
		val step = (nusersEnd.toDouble-nusersStart)/(nintervals-1)
		new WorkloadProfile( (1 to nintervals).map( (i:Int) => Math.round(nusersStart + (i-1)*step).toInt ).toList )
	}

	def getSpiked(nintervals:Int, nusersFlat:Int, nusersSpike:Int): WorkloadProfile = {
		new WorkloadProfile( List(nusersFlat,nusersSpike,nusersFlat) )
	}
}

@serializable
case class WorkloadProfile(
	val profile: List[Int]
) {
	private def fraction(a:Double, b:Double, i:Double, min:Double, max:Double):Double = min + (max-min)*(i-a)/(b-a)
	private def interpolateTwo(a:Double, b:Double, n:Int):List[Int] = (0 to n).toList.map( (i:Int)=>Math.round(a + (b-a)*i/n).toInt )

	def addSpike(tRampup:Int, tSpike:Int, tRampdown:Int, tEnd:Int, magnitude:Double): WorkloadProfile = {
		// val maxUsers = profile.reduceLeft( Math.max(_,_) ).toDouble
		val w = profile.zipWithIndex.map( e =>  	 if (e._2<tRampup) 				e._1.toDouble
											else if (e._2>=tRampup&&e._2<tSpike) 	e._1 * fraction(tRampup,tSpike,e._2,1,magnitude).toDouble
											else if (e._2>=tSpike&&e._2<tRampdown) 	e._1 * magnitude.toDouble
											else if (e._2>=tRampdown&&e._2<tEnd) 	e._1 * fraction(tRampdown,tEnd,e._2,magnitude,1).toDouble
											else 				 					e._1.toDouble ).toList
		// val maxw = w.reduceLeft( Math.max(_,_) ).toDouble
		// WorkloadProfile( w.map( (x:Double) => (x/maxw*maxUsers).toInt ) )
		WorkloadProfile( w.map( _.toInt) )
	}

	def interpolate(nSegments:Int): WorkloadProfile = {
		WorkloadProfile( List.flatten( profile.dropRight(1).zip(profile.tail).map( (p:Tuple2[Int,Int]) => interpolateTwo(p._1,p._2,nSegments).dropRight(1) ) ) ::: List(profile.last) )
	}

	def scale(nMaxUsers:Int): WorkloadProfile = {
		val maxw = profile.reduceLeft( Math.max(_,_) ).toDouble
		WorkloadProfile( profile.map( (w:Int) => (w/maxw*nMaxUsers).toInt ) )
	}

	def smooth(length:Int):List[Double] = {
		val n = profile.length
		val length0 = (length-1)/2
		val length1 = length - length0
		val raw = new ListBuffer[Double]() ++ (List.make(length0,profile.first) ++ profile ++ List.make(length1,profile.last)).map(_.toDouble)

		val smooth = new ListBuffer[Double]()
		for (i <- 0 to (n-1))
			smooth += raw.slice(i,i+length).reduceLeft(_+_)/length
		smooth.toList
	}

	/**
	* use a "max" filter of width 'length' on the workload profile
	*/
	def filterMax(length:Int):WorkloadProfile = {
		val n = profile.length
		val length0 = (length-1)/2
		val length1 = length - length0
		val raw = new ListBuffer[Int]() ++ (List.make(length0,profile.first) ++ profile ++ List.make(length1,profile.last)).map(_.toInt)

		val maxed = new ListBuffer[Int]()
		for (i <- 0 to (n-1))
			maxed += raw.slice(i,i+length-1).reduceLeft(Math.max(_,_))
		WorkloadProfile( maxed.toList )
	}

	/**
	* shift workload. positive shift means shifting backwards
	*/
	def shift(shift:Int):WorkloadProfile = {
		val n = profile.length
		val shifted = 
		if (shift>0) { new ListBuffer[Int]() ++ (profile.slice(shift,n) ++ List.make(shift,profile.last)).map(_.toInt)
		} else { new ListBuffer[Int]() ++ (List.make(-shift,profile.first) ++ profile.slice(0,n+shift)).map(_.toInt) }
		WorkloadProfile( shifted.toList )
	}

	// divide two workload profiles (don't return WorkloadProfile because results are doubles)
	def divide(that:WorkloadProfile):List[Double] = {
		this.profile.zip(that.profile).map( p => p._1.toDouble/p._2 )
	}

	override def toString(): String = profile.toString()
}


@serializable
abstract class SCADSKeyGenerator() {
	def generateKey(): org.apache.avro.generic.GenericData.Record
}

/**
* Create integer keys from a uniform distribution
* from minkey to maxkey
*/
@serializable
case class UniformKeyGenerator(
	val minKey: Int,
	val maxKey: Int
) extends SCADSKeyGenerator() {
	override def generateKey(): org.apache.avro.generic.GenericData.Record = {
		if (WorkloadDescription.rnd_thread.get == null) { 
			val server = java.net.InetAddress.getLocalHost().getHostName
			val thread_name = Thread.currentThread().getName()
			WorkloadDescription.rnd_thread.set(new java.util.Random( (server+thread_name).hashCode.toLong+System.currentTimeMillis )) 
		}
		IntRec( WorkloadDescription.rnd_thread.get.nextInt(maxKey-minKey) + minKey ).toGenericRecord
	}
}

@serializable
abstract class SCADSRequestGenerator(val mix:MixVector) {
	def generateRequest(): ScadsRequest
}

@serializable
case class FixedSCADSRequestGenerator(
	override val mix: MixVector,
	val keyGenerator: SCADSKeyGenerator
	//val value:org.apache.avro.generic.GenericData.Record
) extends SCADSRequestGenerator(mix) {

	def generateRequest(): ScadsRequest = {
		mix.sampleRequestType match {
			case "get" => ScadsGet(keyGenerator.generateKey)
			case "put" => ScadsPut(keyGenerator.generateKey,StringRec("x"*256).toGenericRecord)
		}
	}
}
