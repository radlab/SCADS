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
	var numStorageNodes: Int

	def getCardinalityList(queryType: Option[String]): List[Int]
}

case class GenericClusterParams(
  var clusterAddress: String,
	var numStorageNodes: Int,
	var baseCardinality: Int,
	var dataLowerBound: Int
) extends AvroRecord with ClusterParams {
	def getNumDataItems: Int = {
		getMaxCardinality*10
	}	

  def getMaxCardinality: Int = {
		getCardinalityList(None).sortWith(_ > _).head
	}
	
	def getCardinalityList(queryType: Option[String]): List[Int] = {
		((baseCardinality*0.5).toInt :: (baseCardinality*0.75).toInt :: baseCardinality :: baseCardinality*2 :: baseCardinality*10 :: Nil)
	}
}


case class ScadrClusterParams(
  var clusterAddress: String,
	var numStorageNodes: Int,
	var numLoadClients: Int,
	var numPerPage: Int = 10,
	var numUsers: Int = 100000,
	var numThoughtsPerUser: Int = 100,
	var numSubscriptionsPerUser: Int = 100
) extends AvroRecord with ClusterParams {
  def getCardinalityList(queryType: Option[String]): List[Int] = {
    val cardinalityList = queryType match {
      case Some(q) => 
        q match {
          case "findUser" => List(1)
          case "myThoughts" => getCardinalityListFromMax(numPerPage)
          case "usersFollowedBy" => getCardinalityListFromMax(numSubscriptionsPerUser)
          case "thoughtstream" => getCardinalityListFromMax(numSubscriptionsPerUser)  // actually want to vary # thoughts displayed too
          case "usersFollowing" => getCardinalityListFromMax(numPerPage)
        }
      case None => println("no query specified")
        Nil
    }
    cardinalityList
  }
  
  def getCardinalityListFromMax(max: Int): List[Int] = {
    max :: (max/2).toInt :: (max/10).toInt :: (max/20).toInt :: (max/100).toInt :: (max/200).toInt :: (max/1000).toInt :: Nil
  }
}