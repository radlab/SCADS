package edu.berkeley.cs
package scads
package piql
package modeling

import comm._
import storage._
import perf._
import deploylib.ec2._
import deploylib.mesos._
import piql.scadr._
import perf.scadr._
import avro.marker._
import avro.runtime._

object ExperimentUtil {
  import scala.collection.mutable.HashSet
  import Experiments.resultsCluster
    
  val results = resultsCluster.getNamespace[Result]("queryRunnerResults")
  def downloadResults: Unit = {
    val outfile = AvroOutFile[Result]("QueryRunnerResults.avro")
    results.iterateOverRange(None, None).foreach(outfile.append)
    outfile.close
  }
  
  def allResults = AvroInFile[Result]("QueryRunnerResults.avro")
  
  def goodResults = allResults.filter(_.failedQueries < 200)
			.filterNot(_.iteration == 1)
			.filter(_.clientConfig.iterationLengthMin == 10)
			.filter(_.clientConfig.numClients == 50)

  def experimentResults = allResults.filter(_.clientConfig.experimentAddress contains "experiment0000000164")
  
  def goodExperimentResults = experimentResults.filter(_.failedQueries < 200)
			.filterNot(_.iteration == 1)
			.filter(_.clientConfig.iterationLengthMin == 10)
  
  // get all of the querytypes in this results set
  def queryTypes(results: Seq[Result] = goodResults.toSeq):HashSet[String] = {
    val set = new HashSet[String]()
    results.foreach(set += _.queryDesc.queryName)
    set
  }
  
  def queryDescriptions(results: Seq[Result]) = {
    results.groupBy(_.queryDesc).map { case(queryDesc, res) => println(queryDesc) }
  }
  
  def queryTypeQuantile(results: Seq[Result] = goodResults.toSeq, quantile: Double = 0.90) =
    results.groupBy(_.queryDesc).map {
      case (queryDesc, results) => (queryDesc, results.map(_.responseTimes)
						.reduceLeft(_ + _)
						.map(_.quantile(quantile)))
    }

  def queryTypeHistogram(results: Seq[Result]) = {
    results.groupBy(_.queryDesc).map {
      case (queryDesc, results) => (queryDesc, results.map(_.responseTimes)
						.reduceLeft(_ + _))
		}
  }

  def queryTypeStddev(results: Seq[Result] = goodResults.toSeq) = 
    results.groupBy(_.queryDesc).map {
      case (queryDesc, results) => (queryDesc, results.map(_.responseTimes)
					  .reduceLeft(_ + _)
					  .map(_.stddev))
    }
    
  def queryTypeQuantileAllHistograms(results: Seq[Result] = goodResults.toSeq, quantile: Double = 0.90) = 
    results.groupBy(_.queryDesc).map {
      case (queryDesc, results) => (queryDesc, results.map(_.responseTimes)
                                                      .map(_.quantile(quantile))
                                                      .foldLeft(Histogram(1,1000))(_ add _))
    }

  def queryTypePerIterationHistograms(results: Seq[Result]) = {
    results.groupBy(result => (result.queryDesc, result.iteration)).map {
      case (prefix, resultValues) => (prefix, resultValues.map(_.responseTimes).reduceLeft(_ + _))
    }
  }

  def queryTypeQuantileAllHistogramsMedian(results: Seq[Result] = goodResults.toSeq, quantile: Double = 0.90) = {
    queryTypeQuantileAllHistograms(results, quantile).map {
      case (queryDesc, hist) => (queryDesc, hist.median)
    }
  }
  
  def queryTypeQuantileAllHistogramsStddev(results: Seq[Result] = goodResults.toSeq, quantile: Double = 0.90) = {
    queryTypeQuantileAllHistograms(results, quantile).map {
      case (queryDesc, hist) => (queryDesc, hist.stddev)
    }
  }
  
  def quantileCsv(results: Seq[Result] = goodResults.toSeq, quantile: Double = 0.90, queryName: String) = {
	  val quantiles = queryTypeQuantile(results, quantile).filter(_._1.queryName == queryName)
    dataCsv(quantiles)
	}
	
	def stddevCsv(results: Seq[Result] = goodResults.toSeq, queryName: String) = {
	  val stddev = queryTypeStddev(results).filter(_._1.queryName == queryName)
	  dataCsv(stddev)
	}
	
	def dataCsv(data:Map[QueryDescription, Any]) = {
	  data.map(i => {
	    var line:List[String] = Nil
	    line = i._1.queryName :: line
	    
	    (1 to i._1.parameters.length).foreach(j =>
	      line = i._1.parameters(j-1).toString :: line
	    )
	    
	    val num = i._2 match {
	      case a: Option[Any] => a.get
	      case b: Any => b
	      case _ =>
	    }
	    line = num.toString :: line
	    println(line.reverse.mkString(","))
	  })
	}
	
	def thoughtstreamQuantileCsv(results: Seq[Result] = goodResults.toSeq, quantile: Double = 0.90) = {
	  println("queryName,numSubs,numPerPage,latency")
	  quantileCsv(results, quantile, "thoughtstream")
	}
	
	def myThoughtsQuantileCsv(results: Seq[Result] = goodResults.toSeq, quantile: Double = 0.90) = {
	  println("queryName,numPerPage,latency")
	  quantileCsv(results, quantile, "myThoughts")
	}

	def usersFollowedByQuantileCsv(results: Seq[Result] = goodResults.toSeq, quantile: Double = 0.90) = {
	  println("queryName,numSubs,latency")
	  quantileCsv(results, quantile, "usersFollowedBy")
	}
	
}
