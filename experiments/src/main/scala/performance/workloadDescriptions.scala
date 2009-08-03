package performance

import edu.berkeley.xtrace._
import edu.berkeley.cs.scads.thrift._
import edu.berkeley.cs.scads.nodes._
import edu.berkeley.cs.scads.keys._
import edu.berkeley.cs.scads.placement._
import edu.berkeley.cs.scads.client._
import org.apache.thrift.transport.{TFramedTransport, TSocket}
import org.apache.thrift.protocol.{TBinaryProtocol,XtBinaryProtocol}

import java.io._
import java.net._

object WorkloadGenerators {
	
	def createSimpleLinearGetPutGetsetWorkload(getProb:Double, getsetProb:Double, getsetLength:Int, namespace:String, totalUsers:Int, userStartDelay:Int, thinkTime:Int):WorkloadDescription = {
		// create sample workload description
		val mix = Map("get"->getProb,"getset"->getsetProb,"put"->(1-getProb-getsetProb))
		val parameters = Map("get"->Map("minKey"->"0","maxKey"->"10000","namespace"->namespace),
							 "put"->Map("minKey"->"0","maxKey"->"10000","namespace"->namespace),
							 "getset"->Map("minKey"->"0","maxKey"->"10000","namespace"->namespace,"setLength"->getsetLength.toString)
							)
		val reqGenerator = new SimpleSCADSRequestGenerator(mix,parameters)		
		var intervals = new scala.collection.mutable.ListBuffer[WorkloadIntervalDescription]
		
		for (nusers <- 1 to totalUsers) intervals += new WorkloadIntervalDescription(nusers, userStartDelay, reqGenerator)
		new WorkloadDescription(thinkTime, intervals.toList)
	}
	def flatWorkload(getProb:Double, getsetProb:Double, getsetLength:Int, maxKey:String,namespace:String, totalUsers:Int, num_minutes:Int, thinkTime: Int) = {
		// how many minutes to run test flat workload: all users start making requests at the same time
		val mix = Map("get"->getProb,"getset"->getsetProb,"put"->(1-getProb-getsetProb))
		val parameters = Map("get"->Map("minKey"->"0","maxKey"->maxKey,"namespace"->namespace),
							 "put"->Map("minKey"->"0","maxKey"->maxKey,"namespace"->namespace),
							 "getset"->Map("minKey"->"0","maxKey"->maxKey,"namespace"->namespace,"setLength"->getsetLength.toString)
							)
		val reqGenerator = new SimpleSCADSRequestGenerator(mix,parameters)
		var intervals = new scala.collection.mutable.ListBuffer[WorkloadIntervalDescription]
		val interval = new WorkloadIntervalDescription(totalUsers, num_minutes*60000, reqGenerator)
		intervals += interval

		new WorkloadDescription(thinkTime, intervals.toList)
	}

	def increaseDataSizeWorkload(namespace:String, totalUsers:Int, dataStartDelay_minutes:Int, thinkTime:Int):WorkloadDescription = {
		val sizes = List[Int](40000,60000,80000,100000,120000,140000,160000,180000,200000,220000,240000,260000,280000,300000)
		val mix = Map("get"->1.0,"getset"->0.0,"put"->0.0)
		var intervals = new scala.collection.mutable.ListBuffer[WorkloadIntervalDescription]
		
		sizes.foreach((size)=> {
			val parameters = Map("get"->Map("minKey"->"0","maxKey"->size.toString,"namespace"->namespace),
								 "put"->Map("minKey"->"0","maxKey"->size.toString,"namespace"->namespace),
								 "getset"->Map("minKey"->"0","maxKey"->size.toString,"namespace"->namespace,"setLength"->"0")
								)
			val reqGenerator = new SimpleSCADSRequestGenerator(mix,parameters)
			val interval = new WorkloadIntervalDescription(totalUsers, dataStartDelay_minutes*60000, reqGenerator)
			intervals += interval
		})
		new WorkloadDescription(thinkTime, intervals.toList)
	}

}
