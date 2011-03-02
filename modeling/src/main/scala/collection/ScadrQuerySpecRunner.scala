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
          case "localUserThoughtstream" =>
            new OptimizedQuery("localuserThoughtstream",
                  LocalStopAfter(ParameterLimit(2,100),
                    IndexMergeJoin(client.thoughts,
            		       AttributeValue(0,0) :: Nil,
            		       AttributeValue(1,1) :: Nil,
            		       ParameterLimit(2,100),
            		       false,
                       LocalUserOperator())),
                       executor)  // has to be LocalUserExecutor
          case "mySubscriptions" =>
            (client.subscriptions.where("subscriptions.owner".a === (0.?))
      		    .limit(1.?, p.numSubscriptionsPerUser)).toPiql("mySubscriptions")
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
          case "mySubscriptions" => query(getRandomUsername, cardinality).length
        }
    
        checkResultLength(resultLength, getExpectedResultLength(cardinality))
      }
      case _ =>
    }
  }

  def callThoughtstream(numSubscriptionsPerUser:Int, numPerPage:Int) = {
    params.clusterParams match {
      case p:ScadrClusterParams => {
        queryType match {
          case "thoughtstream" => {
            val resultLength = query(getRandomUsername, numSubscriptionsPerUser, numPerPage).length
            if (resultLength != numPerPage)
              throw new RuntimeException("expected cardinality: " + numPerPage.toString + ", got: " + resultLength.toString)
          }
          case "localUserThoughtstream" => {
            val resultLength = query(numSubscriptionsPerUser, p.numUsers, numPerPage).length
            if (resultLength != numPerPage)
              throw new RuntimeException("expected cardinality: " + numPerPage.toString + ", got: " + resultLength.toString)
          }
          case _ => println("query is of type " + queryType + ", so you can't call thoughtstream")
        }
      }
      case _ => println("problem with types")
    }
  }
  
  // unnecessary; "callThoughtstream" now handles both
  def callLocalUserThoughtstream(numSubscriptionsPerUser:Int, numPerPage:Int) = {
    params.clusterParams match {
      case p:ScadrClusterParams => {
        queryType match {
          case "localUserThoughtstream" => {
            val resultLength = query(numSubscriptionsPerUser, p.numUsers, numPerPage).length
            if (resultLength != numPerPage)
              throw new RuntimeException("expected cardinality: " + numPerPage.toString + ", got: " + resultLength.toString)
          }
          case _ => println("query is of type " + queryType + ", so you can't call thoughtstream")
        }
      }
      case _ => println("problem with types")
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
    	    case "mySubscriptions" => currentCardinality
    	  }
	    }
	    case _ => 0
    }
	  expectedResultLength
	}
}