package deploylib

import java.io.File

class RClusterNode(num: Int) extends RemoteMachine {
	val hostname = "r" + num + ".millennium.berkeley.edu"
	val username = "marmbrus"
	val privateKey = new File("/Users/marmbrus/.ssh/id_rsa")
}

object RCluster {
	val nodes = (1 to 40).toList.map(new RClusterNode(_))

	def activeNodes() = {
		val check = nodes.map((n) => (n, Future(n.executeCommand("hostname"))))
		Thread.sleep(5000)
		check.filter((c) => c._2.isDone && c._2.success).map((c) => c._1)
	}
}
