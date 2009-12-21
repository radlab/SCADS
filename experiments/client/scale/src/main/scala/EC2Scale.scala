package scaletest

import deploylib.ec2._
import deploylib.ParallelConversions._

object EC2Scale {
	def main(args: Array[String]): Unit = {
		val nodes = EC2Instance.myInstances
		println("Running Exp on " + nodes.size)
		nodes.pforeach(_.setup)

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
