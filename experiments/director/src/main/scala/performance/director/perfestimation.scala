package scads.director

import performance._

/**
* Represents the performance statistics of different request types. To make the aggregation
* simpler, we report "fraction of requests slower than N ms" for various values of N
*/
case class PerformanceStats(
	duration: Long, // duration of the interval in seconds
	nGets: Int,
	nPuts: Int,
	nGetsets: Int,
	nGetsAbove50: Int,
	nPutsAbove50: Int,
	nGetsetsAbove50: Int,
	nGetsAbove100: Int,
	nPutsAbove100: Int,
	nGetsetsAbove100: Int
) {
	def add(that:PerformanceStats):PerformanceStats = {
		PerformanceStats(Math.max(this.duration,that.duration),this.nGets+that.nGets,this.nPuts+that.nPuts,this.nGetsets+that.nGetsets,
						this.nGetsAbove50+that.nGetsAbove50,this.nPutsAbove50+that.nPutsAbove50,this.nGetsetsAbove50+that.nGetsetsAbove50,
						this.nGetsAbove100+that.nGetsAbove100,this.nPutsAbove100+that.nPutsAbove100,this.nGetsetsAbove100+that.nGetsetsAbove100)
	}
}


abstract class PerformanceEstimator {
	/**
	* Estimate the performance of this configuration under the specified workload
	* for the specified time (in seconds) while executing the actions
	*/
	def estimatePerformance(config:SCADSconfig, workload:WorkloadHistogram, durationInSec:Int, actions:List[Action]): PerformanceStats
	def estimateApproximatePerformance(config:SCADSconfig, workload:WorkloadHistogram, durationInSec:Int, actions:List[Action], quantile:Double): PerformanceStats
}

object PerformanceEstimator {
	/**
	* Determine the histogram ranges that each server contains. Overlaps possible.
	*/
	def getServerHistogramRanges(config:SCADSconfig, workload:WorkloadHistogram):Map[String,List[DirectorKeyRange]] = {
		val servers = config.storageNodes
		val rangeStats = workload.rangeStats
		val histRanges = rangeStats.keySet.toList.sort(_.minKey<_.minKey)

		val serversOrdered = servers.keySet.toList.sort( servers(_).minKey<servers(_).minKey ).toArray
		var serverStartI = 0

		// compute mapping from servers' ranges to histogram ranges that overlap with those ranges
		val serversToHist = scala.collection.mutable.Map[String,scala.collection.mutable.Buffer[DirectorKeyRange]]()

		for (hr <- histRanges) {
			var go = true
			var found = false
			var i = serverStartI
			while (go && i<servers.size) {
				if (hr.overlaps(servers(serversOrdered(i)))) {
					serversToHist(serversOrdered(i)) = serversToHist.getOrElse(serversOrdered(i),new scala.collection.mutable.ListBuffer[DirectorKeyRange]())+hr
				} else if ( hr.maxKey<=servers(serversOrdered(i)).minKey ) {
					go = false
				} else {
					serverStartI = serverStartI+1
				}
				i = i+1
			}
		}
		Map[String,List[DirectorKeyRange]](serversToHist.toList map {entry => (entry._1, entry._2.toList)} : _*)
	}

	// TODO: make sure this works even if the server ranges don't align nicely
	def estimateServerWorkload(config:SCADSconfig, workload:WorkloadHistogram):Map[String,WorkloadFeatures] = {
		val servers = config.storageNodes
		val serversOrdered = servers.keySet.toList.sort( servers(_).minKey<servers(_).minKey ).toArray
		var serverStartI = 0

		val rangeStats = workload.rangeStats
		val histRanges = rangeStats.keySet.toList.sort(_.minKey<_.minKey)

		// compute mapping from histogram ranges to servers that overlap with those ranges
		val histToServers = scala.collection.mutable.Map[DirectorKeyRange,scala.collection.mutable.Buffer[String]]()
		for (hr <- histRanges) {
			var go = true
			var i = serverStartI
			while (go && i<servers.size) {
/*				println(hr+"  "+serversOrdered(i)+"   overlap="+hr.overlaps(servers(serversOrdered(i)))+"   serverStartI="+serverStartI)*/
				if (hr.overlaps(servers(serversOrdered(i)))) {
					histToServers(hr) = histToServers.getOrElse(hr,new scala.collection.mutable.ListBuffer[String]())+serversOrdered(i)
				} else if ( hr.maxKey<=servers(serversOrdered(i)).minKey ) {
					go = false
				} else {
					serverStartI = serverStartI+1
				}
				i = i+1
			}
		}
/*		histToServers.keySet.toList.sort(_.minKey<_.minKey).foreach( r=>println(r+"   "+histToServers(r)) )*/

		// estimate workload at each server (assume get from one, put to all replicas)
		val serverWorkload = scala.collection.mutable.Map[String,WorkloadFeatures]()
		for (hr <- histRanges) {
			val ss = histToServers.getOrElse(hr,List())
			for (s <- ss) {
				// add this range's workload to servers responsible, splitting get() amongst replicas and applying any put() restrictions
				val restrict = config.putRestrictions.getOrElse(hr,1.0)
				serverWorkload(s) = serverWorkload.getOrElse(s,WorkloadFeatures(0,0,0)).add(rangeStats(hr).restrictAndSplit(ss.size,restrict,0.0))
			}
		}
		Map[String,WorkloadFeatures]()++serverWorkload
	}

	def test() {
/*		import performance._
		import scads.director._
*/
		val c0 = SCADSconfig.getInitialConfig( DirectorKeyRange(0,10000) )
		val a0 = SplitInTwo( c0.storageNodes.keySet.toList.first,5000 )
		val c1 = a0.preview(c0)
		val a1_0 = SplitInTwo( c1.storageNodes.keySet.toList(0),2500 )
		val a1_1 = SplitInTwo( c1.storageNodes.keySet.toList(1),7500 )
		val c2_0 = a1_0.preview(c1)
		val c2 = a1_1.preview(c2_0)
		val a2 = Replicate( c2.storageNodes.find( _._2==DirectorKeyRange(0,2500) ).get._1, 2 )
		val c3 = a2.preview(c2)
		val a3 = MergeTwo( c3.storageNodes.find( _._2==DirectorKeyRange(2500,5000) ).get._1, c3.storageNodes.find( _._2==DirectorKeyRange(5000,7500) ).get._1)
		val c4 = a3.preview(c3)

		val w = WorkloadGenerators.flatWorkload(0.95,0.0,0,"10000","perfTest256",100,2,10)
		val hist = WorkloadHistogram.create(w.workload(0),10000,100,10000)
/*		val pm = L1PerformanceModel("http://scads.s3.amazonaws.com/perfmodels/l1model_getput_1.0.RData")*/
		val pm = L1PerformanceModel("/Users/bodikp/Downloads/l1model_getput_1.0.RData")
		val estimator = SimplePerformanceEstimator(pm)
		estimator.estimatePerformance(c4,hist,1,null)

		hist.rangeStats.keySet.toList.sort(_.minKey<_.minKey).foreach( r=>println(r+"   "+hist.rangeStats(r)) )
	}
}

case class SimplePerformanceEstimator(
	val perfmodel:PerformanceModel
) extends PerformanceEstimator {

	/**
	* predict performance when running 'workload' on 'servers' for 'duration' seconds and also executing 'actions'
	*/
	def estimatePerformance(config:SCADSconfig, workload:WorkloadHistogram, durationInSec:Int, actions:List[Action]): PerformanceStats = {
		val servers = config.storageNodes
		val serverWorkload = PerformanceEstimator.estimateServerWorkload(config,workload)
/*		serverWorkload.map( x=>(x._1,x._2) ).toList.sort(_._1<_._1).foreach( x=> println(x._1+"   "+x._2) )*/

		var stats = PerformanceStats(durationInSec,0,0,0,0,0,0,0,0,0)
		for (s <- servers.keySet) {
			if (!serverWorkload.contains(s)) {
				println("Couldn't find "+s+" in serverWorkload, which has: "+serverWorkload.keys.toList.mkString("(",",",")"))
				println("Histogram has ranges: \n"+workload.rangeStats.keySet.toList.sort(_.minKey<_.minKey).mkString("",",",""))
				println("Config has: \n"+config.rangeNodes.toList.sort(_._1.minKey<_._1.minKey).mkString("","\n","\n-------\n"))
			} else {
				val w = serverWorkload(s)
				val getLatencies = perfmodel.sample(Map("type"->"get","getw"->w.getRate.toString,"putw"->w.putRate.toString),(w.getRate*durationInSec).toInt)
				val putLatencies = perfmodel.sample(Map("type"->"put","getw"->w.getRate.toString,"putw"->w.putRate.toString),(w.putRate*durationInSec).toInt)
				stats = stats.add(PerformanceStats(durationInSec,getLatencies.size,putLatencies.size,0,getLatencies.filter(_>50).size,putLatencies.filter(_>50).size,0,getLatencies.filter(_>100).size,putLatencies.filter(_>100).size,0))
			}
		}
		stats
	}

	/**
	* predict performance when running 'workload' on 'servers' for 'duration' seconds and also executing 'actions'
	*/
	def estimateApproximatePerformance(config:SCADSconfig, workload:WorkloadHistogram, durationInSec:Int, actions:List[Action], quantile:Double): PerformanceStats = {
		val servers = config.storageNodes
		val serverWorkload = PerformanceEstimator.estimateServerWorkload(config,workload)

		var stats = PerformanceStats(durationInSec,0,0,0,0,0,0,0,0,0)
		for (s <- servers.keySet) {
			if (!serverWorkload.contains(s)) {
				println("Couldn't find "+s+" in serverWorkload, which has: "+serverWorkload.keys.toList.mkString("(",",",")"))
				println("Histogram has ranges: \n"+workload.rangeStats.keySet.toList.sort(_.minKey<_.minKey).mkString("",",",""))
				println("Config has: \n"+config.rangeNodes.toList.sort(_._1.minKey<_._1.minKey).mkString("","\n","\n-------\n"))
			} else {
				val w = serverWorkload(s)
				val getLatency = perfmodel.estimateLatency(Map("type"->"get","getw"->w.getRate.toString,"putw"->w.putRate.toString), quantile)
				val putLatency = perfmodel.estimateLatency(Map("type"->"put","getw"->w.getRate.toString,"putw"->w.putRate.toString), quantile)

				val (ngets, ngets_50, ngets_100) = if (getLatency>100) (1000,1000,1000) else if (getLatency>50) (1000,1000,0) else (1000,0,0)
				val (nputs, nputs_50, nputs_100) = if (putLatency>100) (1000,1000,1000) else if (putLatency>50) (1000,1000,0) else (1000,0,0)
				stats = stats.add(PerformanceStats(durationInSec,ngets,nputs,0,ngets_50,nputs_50,0,ngets_100,nputs_100,0))
			}
		}
		stats
	}

}
