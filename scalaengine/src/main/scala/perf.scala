package edu.berkeley.cs.scads

import edu.berkeley.cs.scads.thrift._
import edu.berkeley.cs.scads.nodes.StorageNode
import scala.concurrent.ops._

object PerfTest {
        def main(args: Array[String]) = {
                val node = new StorageNode(args(0), args(1).toInt)
                val rec = new Record(null, "*" * 1024)

                (1 to 1024).foreach((i) => {
                       rec.setKey(i.toString)
                       node.useConnection(_.put("perfTest", rec))
                })

                var continue = true
                val results = (1 to 10).toList.map((i) => {
			future {
				var count = 0
				val startTime = System.currentTimeMillis()
				while(continue) {
					node.useConnection(_.get("perfTest", "1"))
					count += 1
				}
				val endTime = System.currentTimeMillis()

				(startTime, endTime, count)
			}
                })

		val startTime = System.currentTimeMillis()

		while((startTime + 1000*60) > System.currentTimeMillis()) {
			Thread.sleep(1000)
		}
		continue = false

		println(results.map(_()))

        }
}
