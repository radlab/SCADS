package edu.berkeley.cs
package scads
package piql
package modeling

import deploylib.mesos._
import comm._
import storage._
import piql._
import piql.scadr._
import perf._
import avro.marker._
import avro.runtime._

import net.lag.logging.Logger
import java.io.File
import java.net._

import scala.collection.JavaConversions._
import scala.collection.mutable._

import com.amazonaws.services.sns._
import com.amazonaws.auth._
import com.amazonaws.services.sns.model._

class ScadrQuerySpecRunner(val params: RunParams)(implicit executor: QueryExecutor) extends QuerySpecRunner {
  val queryType = params.queryType
  var query:OptimizedQuery = null
  var client:ScadrClient = null
  
  // Note:  we already set up the namespaces in the ScadrDataLoaderTask
  override def setupNamespacesAndCreateQuery(cluster: ExperimentalScadsCluster) = {
    params.clusterParams match {
      case p:ScadrClusterParams => {
        client = new ScadrClient(cluster, executor, p.numSubscriptionsPerUser)

        query = queryType match {
          case "findUser" => client.users.where("username".a === (0.?)).toPiql("findUser")
          case "myThoughts" =>
            (client.thoughts.where("thoughts.owner".a === (0.?))
        	    .sort("thoughts.timestamp".a :: Nil, false)
        	    .limit(1.?, p.numPerPage)
              ).toPiql("myThoughts")
          case "usersFollowedBy" =>
            (client.subscriptions.where("subscriptions.owner".a === (0.?))
        	    .limit(1.?, p.numSubscriptionsPerUser)
        	    .join(client.users)
        	    .where("subscriptions.target".a === "users.username".a)
              ).toPiql("usersFollowedBy")
          case "thoughtstream" =>   // how to vary both of these?  just focus on numSubscriptionsPerUser
            (client.subscriptions.where("subscriptions.owner".a === (0.?))
        		  .limit(1.?, p.numSubscriptionsPerUser)
        		  .join(client.thoughts)
        		  .where("thoughts.owner".a === "subscriptions.target".a)
        		  .sort("thoughts.timestamp".a :: Nil, false)
        		  .limit(2.?, p.numPerPage)
              ).toPiql("thoughtstream")
          case "usersFollowing" =>  
            (client.subscriptions.where("subscriptions.target".a === (0.?))
            	.limit(1.?, p.numPerPage)
            	.join(client.users)
            	.where("users.username".a === "subscriptions.owner".a)
              ).toPiql("usersFollowing")
        }
        query
      }
      case _ => null
    }

  }
  
  def callQuery(cardinality: Int) = {
    params.clusterParams match {
      case p:ScadrClusterParams => {
        val resultLength = queryType match {
          case "findUser" => query(getRandomUsername).length
          case "myThoughts" => query(getRandomUsername, cardinality).length
          case "usersFollowedBy" => query(getRandomUsername, cardinality).length
          case "thoughtstream" => query(getRandomUsername, cardinality, p.numPerPage).length
          case "usersFollowing" => query(getRandomUsername, cardinality).length
        }
    
        checkResultLength(resultLength, getExpectedResultLength(cardinality))
      }
      case _ =>
    }
  }
  
  def getRandomUsername:String = {
    def toUser(idx: Int) = "User%010d".format(idx)

    val username = params.clusterParams match {
      case p:ScadrClusterParams => {
        toUser(scala.util.Random.nextInt(p.numUsers) + 1) // must be + 1 since users are indexed starting from 1
      }
      case _ => println("problem with types")
        ""
    }
    username
  }
  
  def getLimitList(currentCardinality:Int):List[Int] = Nil
	
	def getExpectedResultLength(currentCardinality:Int):Int = {
	  val expectedResultLength = params.clusterParams match {
	    case p:ScadrClusterParams => {
    	  queryType match {
    	    case "findUser" => 1
    	    case "myThoughts" => currentCardinality
    	    case "usersFollowedBy" => currentCardinality
    	    case "thoughtstream" => p.numPerPage
    	    case "usersFollowing" => currentCardinality
    	  }
	    }
	    case _ => 0
    }
	  expectedResultLength
	}
}