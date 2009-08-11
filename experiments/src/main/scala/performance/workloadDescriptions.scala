package performance

import edu.berkeley.xtrace._
import edu.berkeley.cs.scads.thrift._
import edu.berkeley.cs.scads.nodes._
import edu.berkeley.cs.scads.keys._
import edu.berkeley.cs.scads.placement._
import edu.berkeley.cs.scads.client._
import org.apache.thrift.transport.{TFramedTransport, TSocket}
import org.apache.thrift.protocol.{TBinaryProtocol,XtBinaryProtocol}

import java.io._
import java.net._
import scala.io._
import scala.collection.mutable.ListBuffer

object WorkloadGenerators {
	
	def flatWorkload(getProb:Double, getsetProb:Double, getsetLength:Int, maxKey:String, namespace:String, totalUsers:Int, num_minutes:Int, thinkTime: Int):WorkloadDescription = {
		val wProf = WorkloadProfile.getFlat(1,totalUsers)
		val mix = new MixVector(Map("get"->getProb,"getset"->getsetProb,"put"->(1-getProb-getsetProb)))
		val parameters = Map("get"->Map("minKey"->"0","maxKey"->maxKey,"namespace"->namespace),
							 "put"->Map("minKey"->"0","maxKey"->maxKey,"namespace"->namespace),
							 "getset"->Map("minKey"->"0","maxKey"->maxKey,"namespace"->namespace,"setLength"->getsetLength.toString) )
		val reqGenerators = List( new SimpleSCADSRequestGenerator(mix,parameters) )
		val durations = List( num_minutes*60*1000 )
		WorkloadDescription.create(wProf,durations,reqGenerators,thinkTime)
	}	

	def linearWorkload(getProb:Double, getsetProb:Double, getsetLength:Int, maxKey:String, namespace:String, totalUsers:Int, userStartDelay:Int, thinkTime:Int):WorkloadDescription = {
		val wProf = WorkloadProfile.getLinear(totalUsers,1,totalUsers)
		val mix = new MixVector(Map("get"->getProb,"getset"->getsetProb,"put"->(1-getProb-getsetProb)))
		val parameters = Map("get"->Map("minKey"->"0","maxKey"->maxKey,"namespace"->namespace),
							 "put"->Map("minKey"->"0","maxKey"->maxKey,"namespace"->namespace),
							 "getset"->Map("minKey"->"0","maxKey"->maxKey,"namespace"->namespace,"setLength"->getsetLength.toString) )
		val reqGenerators = List.make(totalUsers,new SimpleSCADSRequestGenerator(mix,parameters))		
		val durations = List.make(totalUsers,userStartDelay)
		WorkloadDescription.create(wProf,durations,reqGenerators,thinkTime)
	}
	
	def diurnalWorkload(mix:MixVector, getsetLength:Int, namespace:String, thinkTime:Int, nVirtualDays:Double, nMinutes:Int, maxNUsers:Int):WorkloadDescription = {
		val wProf = WorkloadProfile.getEbatesProfile(6*nMinutes,4*24*60,Math.floor(nVirtualDays*24*60/(6*nMinutes)).toInt, maxNUsers)
		val keyGenerator = new ZipfKeyGenerator(1.001,1,10000)
		val reqGenerators = List.make( 6*nMinutes, new FixedSCADSRequestGenerator(mix,keyGenerator,namespace,getsetLength) )
		val durations = List.make(6*nMinutes,10*1000)
		WorkloadDescription.create(wProf,durations,reqGenerators,thinkTime)
	}

	def increaseDataSizeWorkload(namespace:String, totalUsers:Int, dataStartDelay_minutes:Int, thinkTime:Int):WorkloadDescription = {
		val sizes = List[Int](50000,100000,200000,300000,400000,500000,600000,700000,800000,900000,1000000,1100000,1200000,1300000,1400000,1500000,1600000,1700000,1800000,1900000,2000000,2100000,2200000,2300000,2400000,2500000)
		val mix = new MixVector( Map("get"->1.0,"getset"->0.0,"put"->0.0) )
		var intervals = new scala.collection.mutable.ListBuffer[WorkloadIntervalDescription]

		sizes.foreach((size)=> {
			val parameters = Map("get"->Map("minKey"->"0","maxKey"->size.toString,"namespace"->namespace),
								 "put"->Map("minKey"->"0","maxKey"->size.toString,"namespace"->namespace),
								 "getset"->Map("minKey"->"0","maxKey"->size.toString,"namespace"->namespace,"setLength"->"0")
								)
			val reqGenerator = new SimpleSCADSRequestGenerator(mix,parameters)
			val interval = new WorkloadIntervalDescription(totalUsers, dataStartDelay_minutes*60000, reqGenerator)
			intervals += interval
		})
		new WorkloadDescription(thinkTime, intervals.toList)
	}

	def simpleUserSpikeWorkload(getProb:Double, getsetProb:Double, getsetLength:Int, maxKey:String, namespace:String, totalUsers:Int, flatUsers:Int, interval_min:Int, thinkTime: Int):WorkloadDescription = {
		val num_intervals = 3
		val wProf = WorkloadProfile.getSpiked(interval_min,flatUsers,totalUsers)
		val mix = new MixVector(Map("get"->getProb,"getset"->getsetProb,"put"->(1-getProb-getsetProb)))
		val parameters = Map("get"->Map("minKey"->"0","maxKey"->maxKey,"namespace"->namespace),
							 "put"->Map("minKey"->"0","maxKey"->maxKey,"namespace"->namespace),
							 "getset"->Map("minKey"->"0","maxKey"->maxKey,"namespace"->namespace,"setLength"->getsetLength.toString) )
		val reqGenerators = List.make(num_intervals,new SimpleSCADSRequestGenerator(mix,parameters))
		val durations = List(interval_min*60*1000,interval_min*2*60*1000,interval_min*60*1000)
		WorkloadDescription.create(wProf,durations,reqGenerators,thinkTime)
	}

	def wikipedia(mix:MixVector, namespace:String, nHoursSkip:Int, nHoursDuration:Int, nMinutesDuration:Int, maxNUsers:Int, nKeys:Int):WorkloadDescription = {
		val dataset = "wikipedia_20090623_20090630"
		val nIntervals = 6 * nMinutesDuration
		val durations = List.make(nIntervals,10)
		val profile = WorkloadProfile.getWikipedia( nIntervals, dataset, nHoursSkip, nHoursDuration, maxNUsers )
		val reqGenerators = SCADSKeyGenerator.wikipediaKeyProfile(nIntervals, dataset, nHoursSkip, nHoursDuration, nKeys, nHoursSkip + nHoursDuration/2).map( FixedSCADSRequestGenerator(mix,_,namespace,10) ).toList
		WorkloadDescription.create(profile,durations,reqGenerators,10)
	}

	def benchmarkGetsAndPuts(nMinDuration:Int, dataset:String, maxKey:String, minGetP:Double, maxGetP:Double, nGetPSteps:Int, maxUsers:Int, thinkTime:Int):WorkloadDescription = {
		val getProbs = for (i <- 0 to (nGetPSteps-1)) yield { minGetP + (maxGetP-minGetP)/(nGetPSteps-1)*i }
		val intLength = (nMinDuration.toDouble*60*1000 / getProbs.length / maxUsers).toInt
		val workloads = getProbs.map( linearWorkload(_, 0.0, 10, maxKey, dataset, maxUsers, intLength, thinkTime) ).toList
		WorkloadDescription.cat( workloads )
	}
}

