import scaletest._
import deploylib._
import deploylib.rcluster._
import deploylib.xresults._
import deploylib.ec2._
import deploylib.runit._
import deploylib.ParallelConversions._
import edu.berkeley.cs.scads.thrift._
import org.apache.log4j.Logger
import java.io.File

object BdbRawTest {
	val logger = Logger.getLogger("scaletest.bdbraw")
	val jobs = new java.util.concurrent.ConcurrentLinkedQueue[Map[String, String]]

	(1 to 100 by 5).foreach(threads =>
	(100000 to 1000000 by 200000).foreach(keys => jobs.offer(Map("threads" -> threads.toString, "keys" -> keys.toString, "length" -> (5 * 60).toString, "repeat" -> "3"))))

	val executors = EC2Instance.myInstances.map(inst => Future {
		inst.setup()
		var job = jobs.poll()
		while(job != null) {
			logger.info("Running " + job + " on " + inst)
			inst.stopWatches; inst.clearAll

			val test = ScadsDeployment.deployLoadClient(inst, "BdbRaw", job)
			test.watchFailures; test.once
			test.blockTillDown

			XResult.captureDirectory(inst, new File(test.serviceDir, "db"))
			test.captureLog
			job = jobs.poll()
		}
	})
}
