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
import java.io.File
import java.net._

import scala.collection.JavaConversions._
import scala.collection.mutable._

import com.amazonaws.services.sns._
import com.amazonaws.auth._
import com.amazonaws.services.sns.model._

abstract class QuerySpecRunner {
  def setupNamespacesAndCreateQuery(cluster: ExperimentalScadsCluster)
  
  def callQuery(cardinality: Int)
  
  def getLimitList(currentCardinality:Int):List[Int]
	
	def getExpectedResultLength(currentCardinality:Int):Int
	
	def checkResultLength(actualResultLength:Int, expectedResultLength:Int)
}

class GenericQuerySpecRunner(val params: RunParams)(implicit executor: QueryExecutor) extends QuerySpecRunner {
  val queryType = params.queryType
  var query:OptimizedQuery = null
  
  override def setupNamespacesAndCreateQuery(cluster: ExperimentalScadsCluster) = {
    query = queryType match {
      case "getQuery" => 
        val r1 = cluster.getNamespace[R1]("r1")
        r1 ++= (1 to params.dataLowerBound).view.map(i => R1(i))

        r1.where("f1".a === 1).toPiql("getQuery")
      case "getRangeQuery" =>
        val r2 = cluster.getNamespace[R2]("r2")
        r2 ++= (1 to params.dataLowerBound).view.flatMap(i => (1 to params.getNumDataItems).map(j => R2(i,j)))    

        r2.where("f1".a === 1)
            .limit(0.?, params.getMaxCardinality)
            .toPiql("getRangeQuery")      
      case "lookupJoinQuery" =>
        val r1 = cluster.getNamespace[R1]("r1")
        val r2 = cluster.getNamespace[R2]("r2")
        r1 ++= (1 to params.getNumDataItems).view.map(i => R1(i))
        r2 ++= (1 to params.dataLowerBound).view.flatMap(i => (1 to params.getNumDataItems).map(j => R2(i,j)))    
        
        r2.where("f1".a === 1)
            .limit(0.?, params.getMaxCardinality)
            .join(r1)
            .where("r1.f1".a === "r2.f2".a)
            .toPiql("joinQuery")
      case "mergeSortJoinQuery" =>
        val r1 = cluster.getNamespace[R1]("r1")
        val r2 = cluster.getNamespace[R2]("r2")
        val r2Prime = cluster.getNamespace[R2]("r2Prime")
  
        r1 ++= (1 to params.dataLowerBound).view.map(i => R1(i))
        r2 ++= (1 to params.dataLowerBound).view.flatMap(i => (1 to params.getNumDataItems).map(j => R2(i,j)))    
        r2Prime ++= (1 to params.dataLowerBound).view.flatMap(i => (1 to params.getNumDataItems).map(j => R2(i,j)))    
        
        // what to do about these 2 limits?
        r2.where("f1".a === 1)
              .limit(5)
              .join(r2Prime)
              .where("r2.f2".a === "r2Prime.f1".a)
              .sort("r2Prime.f2".a :: Nil)
              .limit(10)
              .toPiql("mergeSortJoinQuery")
    }
  }
  
  override def callQuery(cardinality: Int) = {
    val limitList = getLimitList(cardinality)
    
    val resultLength = queryType match {
      case "getQuery" => query().length
      case "getRangeQuery" => query(limitList.head).length
      case "lookupJoinQuery" => query(limitList.head).length
      case "mergeSortJoinQuery" => query(limitList(0), limitList(1)).length
    }
    
    checkResultLength(resultLength, getExpectedResultLength(cardinality))
  }
  
  override def getLimitList(currentCardinality:Int):List[Int] = {
    val limitList = queryType match {
	    case "getQuery" => List(1)
	    case "getRangeQuery" => List(currentCardinality)
	    case "lookupJoinQuery" => List(currentCardinality)
	    case "mergeSortJoinQuery" => List(currentCardinality, params.dataLowerBound)
	  }
	  limitList
  }
	
	override def getExpectedResultLength(currentCardinality:Int):Int = {
	  val expectedResultLength = queryType match {
	    case "getQuery" => 1
	    case "getRangeQuery" => currentCardinality
	    case "lookupJoinQuery" => currentCardinality
	    case "mergeSortJoinQuery" => params.dataLowerBound
	  }
	  expectedResultLength
	}
	
	override def checkResultLength(actualResultLength:Int, expectedResultLength:Int) = {
	  if (actualResultLength != expectedResultLength)
      throw new RuntimeException("expected cardinality: " + expectedResultLength.toString + ", got: " + actualResultLength.toString)
	}
}