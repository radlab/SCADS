import edu.berkeley.cs.scads.client._
import performance._

val port = 8000
val host = args(0)
val xtrace_on:Boolean = args(1).toBoolean
val minUserId = args(2).toInt
val maxUserId = args(3).toInt

val workloadFile = args(4)
val workload = WorkloadDescription.deserialize(workloadFile)

if (xtrace_on) { System.setProperty("xtrace","") }
System.setProperty("xtrace.reporter","edu.berkeley.xtrace.reporting.TcpReporter")
System.setProperty("xtrace.tcpdest","127.0.0.1:7831")

// set up threads with ids
val threads = (minUserId to maxUserId).toList.map((id) => {
	val agent = new WorkloadAgent(new SCADSClient(host,port), workload, id)
	new Thread(agent)
})

// run the test
for(thread <- threads) thread.start
for(thread <- threads) thread.join




