package edu.berkeley.cs.scads.director

import net.lag.logging.Logger
import edu.berkeley.cs.scads.comm.{StorageService}

abstract class PerformanceEstimator {
	val logger = Logger("perfestimator")
	def perServerViolations(config:ClusterState, workload:WorkloadHistogram, getThreshold:Double, putThreshold:Double, quantile:Double):Map[StorageService,Boolean]
}

case class SimplePerformanceEstimator(
	val perfmodel:PerformanceModel,
	val getSLA:Double,
	val putSLA:Double,
	val slaQuantile:Double,
	val reads:Int
) extends PerformanceEstimator {

	def perServerViolations(config:ClusterState, workload:WorkloadHistogram, getThreshold:Double, putThreshold:Double, quantile:Double):Map[StorageService,Boolean] = {
		val serverWorkload = PerformanceEstimator.estimateServerWorkloadReads(config,workload,reads)
		var slaViolations = Map[StorageService,Boolean]()

		assert(getThreshold==perfmodel.slowThreshold&&putThreshold==perfmodel.slowThreshold,
						"slow thresholds need to match the model")

		for ((s,w) <- serverWorkload) slaViolations += s -> (if (perfmodel.overloaded(w.getRate, w.putRate)) true else false)
		slaViolations
	}
	
	def violationOnServer( config:ClusterState, workload:WorkloadHistogram, server:StorageService ):Boolean = {
		val workloadPerServer = PerformanceEstimator.estimateServerWorkloadReads(config,workload,reads)
		logger.debug("    checking violation on "+server+" workload per server:\n"+workloadPerServer.toList.sort(_._1<_._1).map( p => p._1+" => "+p._2.toString ).mkString("\n"))

		val violationsPerServer = perServerViolations(config,workload,getSLA,putSLA,slaQuantile)
		//logger.debug("looking for viol on %s:\n%s",server.toString,violationsPerServer.mkString(","))
		violationsPerServer( server )
	}
	
	def violationOnServers( config:ClusterState, workload:WorkloadHistogram, servers:Set[StorageService] ):Boolean = {
		val workloadPerServer = PerformanceEstimator.estimateServerWorkloadReads(config,workload,reads)
		logger.debug("workload per server\n%s",workloadPerServer.mkString("\n"))
		//logger.debug("    checking violations on "+servers.toString+" workload per server:\n"+workloadPerServer.toList.sort(_._1<_._1).map( p => p._1+" => "+p._2.toString ).mkString("\n"))

		val violationsPerServer = perServerViolations(config,workload,getSLA,putSLA,slaQuantile)
		violationsPerServer.toList.filter( p => servers.contains(p._1) ).exists(_._2)
	}

	def handleOnOneServer(workload:WorkloadFeatures, getThreshold:Double, putThreshold:Double, quantile:Double):Boolean = {
		val fractionSlow = perfmodel.estimateFractionSlow(workload.getRate, workload.putRate)
		fractionSlow<(1-quantile)
	}
}

case class ConstantThresholdPerformanceEstimator(
	val threshold:Int,
	override val getSLA:Double,
	override val putSLA:Double,
	override val slaQuantile:Double,
	override val reads:Int
) extends SimplePerformanceEstimator(null,getSLA,putSLA,slaQuantile,reads) {

	override def perServerViolations(config:ClusterState, workload:WorkloadHistogram, getThreshold:Double, putThreshold:Double, quantile:Double):Map[StorageService,Boolean] = {
		val serverWorkload = PerformanceEstimator.estimateServerWorkloadReads(config,workload,reads)
		//logger.debug("workload:\n%s",workload.toString)
		//logger.debug("serverWorkload\n%s",serverWorkload.mkString("\n"))
		var slaViolations = Map[StorageService,Boolean]()

		for ((s,w) <- serverWorkload) slaViolations += s -> (
				if ( (w.getRate+ w.putRate) >threshold ) true else false
			)
		slaViolations

	}
	override def handleOnOneServer(workload:WorkloadFeatures, getThreshold:Double, putThreshold:Double, quantile:Double):Boolean = {
		val totalWorkload = workload.getRate + workload.putRate
		totalWorkload < threshold
	}

}

object PerformanceEstimator {
	/**
	* For each server, estimate its workload under the given config
	* Computed by adding workload info for each partition that the server is responsible for
	*/
	def estimateServerWorkloadReads(config:ClusterState, workload:WorkloadHistogram, reads:Int):Map[StorageService,WorkloadFeatures] = {
		// for each partition from config, add up the workload from histogram and divide it among the servers
		val serverWorkload = scala.collection.mutable.Map[StorageService,WorkloadFeatures]()
		
		// for each partition key, compute what a server with that partition would get
		val workloadPerPartition = config.keysToPartitions.map(entry => (entry._1,workload.rangeStats(entry._1) * (reads.toDouble/entry._2.size))) // server's portion of the workload for this key is the total = (reads/#replicas)
		
		// for each server, add up its workload per partition
		config.serversToPartitions.map(entry => (entry._1,entry._2.foldLeft(WorkloadFeatures(0.0,0.0,0.0))((sum,range)=> sum + workloadPerPartition(config.partitionsToKeys(range)))))
	}

}