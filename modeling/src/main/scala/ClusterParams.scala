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

abstract sealed trait ClusterParams extends AvroUnion {
  // abstract fields
  var clusterAddress: String
	var baseCardinality: Int
	var numStorageNodes: Int
	var dataLowerBound: Int
  
  def getMaxCardinality: Int = {
		getCardinalityList.sortWith(_ > _).head
	}
	
	def getCardinalityList: List[Int] = {
		((baseCardinality*0.5).toInt :: (baseCardinality*0.75).toInt :: baseCardinality :: baseCardinality*2 :: baseCardinality*10 :: baseCardinality*100:: Nil)
	}
}

case class GenericClusterParams(
  var clusterAddress: String,
	var baseCardinality: Int,
	var numStorageNodes: Int,
	var dataLowerBound: Int
) extends AvroRecord with ClusterParams {
	def getNumDataItems: Int = {
		getMaxCardinality*10
	}	
}


case class ScadrClusterParams(
  var clusterAddress: String,
	var baseCardinality: Int,
	var numStorageNodes: Int,
	var dataLowerBound: Int = 10,
	var numUsers: Int = 100000,
	var numThoughtsPerUser: Int = 100,
	var numSubscriptionsPerUser: Int = 100
) extends AvroRecord with ClusterParams {
  
}