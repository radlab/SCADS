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
  var clusterParams: ClusterParams,
	var queryType: String,
	var warmupLengthInMinutes: Int = 5, 
	var numQueriesPerCardinality: Int = 1000, 
	var sleepDurationInMs: Int = 100
) extends AvroRecord {
  // should get rid of this
	def getExperimentDescription: String = {
		"experiment had the following params:\n" + toString
	}
	
	// should really have toString method in ClusterParams
	override def toString: String = {
		List(
  		"experiment had the following params:",
			"  clusterAddress: " + clusterParams.clusterAddress.toString,
			"  queryType: " + queryType.toString,
			"  baseCardinality: " + clusterParams.baseCardinality.toString,
			"  warmupLengthInMinutes: " + warmupLengthInMinutes.toString,
			"  numStorageNodes: " + clusterParams.numStorageNodes.toString,
			"  numQueriesPerCardinality: " + numQueriesPerCardinality.toString,
			"  sleepDurationInMs: " + sleepDurationInMs.toString
		).mkString("\n")
	}
}