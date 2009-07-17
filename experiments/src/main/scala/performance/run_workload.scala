package performance

import edu.berkeley.cs.scads.client._

object SCADSWorkloadGenerator {
	val host = "ip-10-251-127-68.ec2.internal"
	val minUserId = 0
	val maxUserId = 9
	val xtrace_on:Boolean = false
	
	if (xtrace_on) { System.setProperty("xtrace","") }
	val workload =  WorkloadAgentTest.linearWorkload(0.9,"perfTest200",10,1000,5000) // takes namespace, read_prob, total_users, user_start_delay_ms, think_time

	//case class TestClient(host: String, port: Int, name: String) extends SCADSClient(host,port)

	// set up threads with ids	
	val threads = (minUserId to maxUserId).toList.map((id) => { 
		val agent = new WorkloadAgent(new SCADSClient(host,8000), workload,id)
		new Thread(agent)
	})

	// run the test
	for(thread <- threads) thread.start
	for(thread <- threads) thread.join
}

	
