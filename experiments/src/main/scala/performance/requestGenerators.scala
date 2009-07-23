package performance

import edu.berkeley.cs.scads.client._
import edu.berkeley.cs.scads.thrift._
import edu.berkeley.cs.scads.nodes._
import edu.berkeley.cs.scads.keys._
import edu.berkeley.cs.scads.placement._


abstract class SCADSRequest(
	val client: ClientLibrary
) {
	def reqType: String
	def execute
}

class SCADSGetRequest(
	override val client: ClientLibrary,
	val namespace: String,
	val key: String
) extends SCADSRequest(client) {
	def reqType: String = "get"
	def execute = {
//		print("executing "+toString)
		val value = client.get(namespace,key).value
//		println("   returned="+value)
		value
	}
	override def toString: String = "get("+namespace+","+key+")"
}

class SCADSPutRequest(
	override val client: ClientLibrary,
	val namespace: String,
	val key: String,
	val value: String
) extends SCADSRequest(client) {
	def reqType: String = "put"
	def execute = {
//		print("executing "+toString)
		val success = client.put(namespace,new Record(key,value))
//		println("   returned="+success)
		success
	}
	override def toString: String = "put("+namespace+","+key+"="+value+")"
}

class SCADSGetSetRequest(
	override val client: ClientLibrary,
	val namespace: String,
	val startKey: String,
	val endKey: String,
	val skip: int,
	val limit: int
) extends SCADSRequest(client) {
	def reqType: String = "getset"
	def execute = {
		client.get_set(namespace,new RecordSet(3,new RangeSet("'"+startKey+"'","'"+endKey+"'",skip,limit),null,null))
	}
}



object SCADSRequestGenerator {
	import java.util.Random
	val rand = new Random()
}

@serializable
abstract class SCADSRequestGenerator {
	def generateRequest(client: ClientLibrary, time: Long): SCADSRequest
}


@serializable
class SimpleSCADSRequestGenerator(
	val mix: Map[String, Double],
	val parameters: Map[String, Map[String, String]]
) extends SCADSRequestGenerator {
	val keyFormat = new java.text.DecimalFormat("000000000000000")
	
	// populate these from 'parameters' to speed up the request generation
	val getMinKey = parameters("get")("minKey").toInt
	val getMaxKey = parameters("get")("maxKey").toInt
	val getNamespace = parameters("get")("namespace")
	
	val putMinKey = parameters("put")("minKey").toInt
	val putMaxKey = parameters("put")("maxKey").toInt
	val putNamespace = parameters("put")("namespace")
	
	val getsetMinKey = parameters("getset")("minKey").toInt
	val getsetMaxKey = parameters("getset")("maxKey").toInt
	val getsetNamespace = parameters("getset")("namespace")
	val getsetSetLength = parameters("getset")("setLength").toInt
	
	def generateRequest(client: ClientLibrary, time: Long): SCADSRequest = {
		getRequestType match {
			case "get" => {
				val key = SCADSRequestGenerator.rand.nextInt( getMaxKey-getMinKey ) + getMinKey
				new SCADSGetRequest(client,getNamespace,keyFormat.format(key))
			}
			case "put" => {
				val key = SCADSRequestGenerator.rand.nextInt( putMaxKey-putMinKey ) + putMinKey
				new SCADSPutRequest(client,putNamespace,keyFormat.format(key),"value")
			}
			case "getset" => {
				val startKey = SCADSRequestGenerator.rand.nextInt( getsetMaxKey-getsetMinKey ) + getsetMinKey
				val endKey = startKey+getsetSetLength
				new SCADSGetSetRequest(client,getsetNamespace,keyFormat.format(startKey),keyFormat.format(endKey),0,getsetSetLength)
			}
		}
	}
	
	def getRequestType(): String = {
		val r = SCADSRequestGenerator.rand.nextDouble()
		var agg:Double = 0

		var reqType = ""
		for (req <- mix.keySet) {
			agg += mix(req)
			if (agg >= r && reqType=="") reqType = req
		}
		reqType
	}	
	
}