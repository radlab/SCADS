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

/*class SCADSGetSetRequest extends SCADSRequest {
	def reqType: String = "getset"
}
*/


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
	
	def generateRequest(client: ClientLibrary, time: Long): SCADSRequest = {
		getRequestType match {
			case "get" => {
				val key = SCADSRequestGenerator.rand.nextInt( getMaxKey-getMinKey+1 ) + getMinKey
				new SCADSGetRequest(client,getNamespace,keyFormat.format(key))
			}
			case "put" => {
				val key = SCADSRequestGenerator.rand.nextInt( putMaxKey-putMinKey+1 ) + putMinKey
				new SCADSPutRequest(client,putNamespace,keyFormat.format(key),"value")
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