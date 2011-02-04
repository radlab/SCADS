package edu.berkeley.cs
package scads
package piql
package modeling

import deploylib.mesos._
import comm._
import storage._
import piql._
import perf._
import avro.marker._
import avro.runtime._

import net.lag.logging.Logger

case class RunParams(
	var clusterAddress: String, 
	var queryType: String,
	var baseCardinality: Int, 
	var warmupLengthInMinutes: Int = 5, 
	var numStorageNodes: Int = 1, 
	var numQueriesPerCardinality: Int = 1000, 
	var sleepDurationInMs: Int = 100,
	var dataLowerBound: Int = 10
) extends AvroRecord {
	def getNumDataItems: Int = {
		getMaxCardinality*10
	}
	
	def getMaxCardinality: Int = {
		getCardinalityList.sortWith(_ > _).head
	}
	
	def getCardinalityList: List[Int] = {
		((baseCardinality*0.5).toInt :: (baseCardinality*0.75).toInt :: baseCardinality :: baseCardinality*2 :: baseCardinality*10 :: baseCardinality*100:: Nil)
	}

  // should get rid of this
	def getExperimentDescription: String = {
		"experiment had the following params:\n" + toString
	}
	
	override def toString: String = {
		List(
  		"experiment had the following params:",
			"  clusterAddress: " + clusterAddress.toString,
			"  queryType: " + queryType.toString,
			"  baseCardinality: " + baseCardinality.toString,
			"  warmupLengthInMinutes: " + warmupLengthInMinutes.toString,
			"  numStorageNodes: " + numStorageNodes.toString,
			"  numQueriesPerCardinality: " + numQueriesPerCardinality.toString,
			"  sleepDurationInMs: " + sleepDurationInMs.toString
		).mkString("\n")
	}
}