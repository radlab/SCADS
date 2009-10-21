// note:  this assumes each key belongs to exactly one storage node
import edu.berkeley.cs.scads.client._
import edu.berkeley.cs.scads.thrift.Record
import performance._

val port = 8000
val host = args(0)
val namespace = args(1)
val minKey = args(2).toDouble // doubles so can divide later
val maxKey = args(3).toDouble
val xtrace_on:Boolean = args(4).toBoolean

if (xtrace_on) System.setProperty("xtrace","")

case class CacheWarmer(startk:Int, endk:Int) extends Runnable {
	override def run() = {
		val client = new SCADSClient(host,port)
		(startk to endk).toList.foreach((key) => {
			client.get(namespace,getKey(key))
			//client.put(namespace,new Record(getKey(key),getValue()))
		})
	}
	def getKey(key: Int):String = {
		val keyFormat = new java.text.DecimalFormat("000000000000000")
		keyFormat.format(key)
	}
	def getValue() = {
		"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
	}
}
// set up the warming threads
val slice = (Math.ceil( (maxKey-minKey)/128 )).toInt // divide the work amongst 128 threads
val threads = (0 to 127).toList.map((id)=>{
	new Thread(new CacheWarmer( id*slice,id*slice+(slice-1) ))
})

// start warming
println("warming... ")
val start = System.currentTimeMillis()
for(thread <- threads) thread.start
for(thread <- threads) thread.join
println("done warming: "+ ( (System.currentTimeMillis()-start)/1000 ).toString +" seconds elapsed")
