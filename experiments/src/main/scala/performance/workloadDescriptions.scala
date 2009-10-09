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
	
	val namespace = "perfTest256"
	val thinkTime = 10
	val mix100 = new MixVector(Map("get"->1.0,"getset"->0.0,"put"->0.0))
	val mix99 = new MixVector(Map("get"->0.99,"getset"->0.0,"put"->0.01))
	val mix98 = new MixVector(Map("get"->0.98,"getset"->0.0,"put"->0.02))
	val mix97 = new MixVector(Map("get"->0.97,"getset"->0.0,"put"->0.03))
	val mix96 = new MixVector(Map("get"->0.96,"getset"->0.0,"put"->0.04))
	
	def stdWorkload3Flat(mix:MixVector, maxUsers:Int, maxKey:Int):WorkloadDescription = {
		val n = 600
		val wProf = WorkloadProfile.getFlat(n,maxUsers).addSpike(n/3,n/3,n*2/3,n*2/3,3.0).addSpike(n*2/3,n*2/3,n,n,1.5)
		val keyGenerator = new ZipfKeyGenerator(1.001,3.456,1,maxKey)
		val reqGenerators = List.make( n, new FixedSCADSRequestGenerator(mix,keyGenerator,namespace,1) )
		val durations = List.make( n, 60*1000 )
		WorkloadDescription.create(wProf,durations,reqGenerators,thinkTime)
	}
	
	def stdWorkloadEbatesWSpike(mix:MixVector, maxUsers:Int, maxKey:Int):WorkloadDescription = {
		val nVirtualDays = 1.5
		val nMinutes = 60
		
		val wProf = WorkloadProfile.getEbatesProfile(6*nMinutes,4*24*60,Math.floor(nVirtualDays*24*60/(6*nMinutes)).toInt, maxUsers).addSpike(180,186,246,300,3.0)
		val keyGenerator = new ZipfKeyGenerator(1.001,3.456,1,maxKey)
		val reqGenerators = List.make( 6*nMinutes, new FixedSCADSRequestGenerator(mix,keyGenerator,namespace,1) )
		val durations = List.make(6*nMinutes,10*1000)
		WorkloadDescription.create(wProf,durations,reqGenerators,thinkTime)
	}
	
	def stdWorkloadEbatesWMixChange(mix0:MixVector, mix1:MixVector, maxUsers:Int, maxKey:Int):WorkloadDescription = {
		val nVirtualDays = 1.5
		val nMinutes = 60
		val nInt = 6*nMinutes

		val mixProfile = WorkloadMixProfile.getStaticMix(nInt,mix0)
				.transition( WorkloadMixProfile.getStaticMix(nInt,mix1), (0.4*nInt).toInt, (0.45*nInt).toInt )
				.transition( WorkloadMixProfile.getStaticMix(nInt,mix0), (0.65*nInt).toInt, (0.75*nInt).toInt )
		
		val wProf = WorkloadProfile.getEbatesProfile(nInt,4*24*60,Math.floor(nVirtualDays*24*60/(nInt)).toInt, maxUsers)
		val keyGenerator = new ZipfKeyGenerator(1.001,3.456,1,maxKey)
		val reqGenerators = mixProfile.profile.map( new FixedSCADSRequestGenerator(_,keyGenerator,namespace,1) ).toList
		val durations = List.make(nInt,10*1000)
		WorkloadDescription.create(wProf,durations,reqGenerators,thinkTime)		
	}
	
	def stdWorkloadFlatWithMixChange(mixes:List[MixVector], maxUsers:Int, maxKey:Int):WorkloadDescription = {
		val n = 100
		val wProf = WorkloadProfile.getFlat(n,maxUsers)
		
		val mixProfile = WorkloadMixProfile.getStaticMix(n,mixes(0))
				.transition( WorkloadMixProfile.getStaticMix(n,mixes(1)), (1/5.0*n).toInt, (2/5.0*n).toInt )
				.transition( WorkloadMixProfile.getStaticMix(n,mixes(2)), (3/5.0*n).toInt, (4/5.0*n).toInt )
		
		val keyGenerator = new ZipfKeyGenerator(1.001,3.456,1,maxKey)
		val reqGenerators = mixProfile.profile.map( new FixedSCADSRequestGenerator(_,keyGenerator,namespace,1) ).toList
		
		val durations = List.make( n, 60*1000 )
		WorkloadDescription.create(wProf,durations,reqGenerators,thinkTime)		
	}
	
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
	
	def diurnalWorkload(mix:MixVector, getsetLength:Int, namespace:String, thinkTime:Int, nVirtualDays:Double, nMinutes:Int, maxNUsers:Int,maxKey:Int):WorkloadDescription = {
		val wProf = WorkloadProfile.getEbatesProfile(6*nMinutes,4*24*60,Math.floor(nVirtualDays*24*60/(6*nMinutes)).toInt, maxNUsers)
		val keyGenerator = new ZipfKeyGenerator(1.001,3.456,1,maxKey)
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

