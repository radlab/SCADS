package scaletest

import deploylib.rcluster._
import deploylib.ParallelConversions._

object RClusterScale {
	def main(args: Array[String]): Unit = {
		val nodes = RCluster.activeNodes.filter(n => !(n.hostname equals "r21.millennium.berkeley.edu")).filter(n => !(n.hostname equals "r20.millennium.berkeley.edu")).filter(n => !(n.hostname equals "r18.millennium.berkeley.edu")).filter(n => !(n.hostname equals "r19.millennium.berkeley.edu"))

		println("Running Exp on " + nodes.size)
		nodes.pforeach(n => {
			n.setupRunit
		})

		println("Begining Test")
		val le = new LoadExp(nodes, 5, true, nodes.size * 1000000)
		le.postTestCollection()

		var re: ReadExp = null
		(1 to 5).foreach(i => {
			re = new ReadExp(nodes, nodes, 35)
			re.postTestCollection()
		})
	}
}
