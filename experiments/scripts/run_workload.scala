import edu.berkeley.cs.scads.client._
import performance._

val port = 8000
val host = args(0)
val xtrace_on:Boolean = args(1).toBoolean
val minUserId = args(2).toInt
val maxUserId = args(3).toInt

val read_prob = args(4).toDouble
val namespace = args(5)
val total_users = args(6).toInt
val start_delay = args(7).toInt
val think =  args(8).toInt

if (xtrace_on) { System.setProperty("xtrace","") }
val workload =  WorkloadAgentTest.linearWorkload(read_prob,namespace,total_users,start_delay,think)

//case class TestClient(host: String, port: Int, name: String) extends SCADSClient(host,port)

// set up threads with ids	
val threads = (minUserId to maxUserId).toList.map((id) => { 
	val agent = new WorkloadAgent(new SCADSClient(host,port), workload,id)
	new Thread(agent)
})

// run the test
for(thread <- threads) thread.start
for(thread <- threads) thread.join



	
