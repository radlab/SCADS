package deploylib.rcluster

import deploylib._
import deploylib.runit._

import java.io.File
import scala.util.matching.Regex

/**
 * Convienence methods for working with the RCluster
 */
object RCluster {
	val nodes = (1 to 40).toList.map(new RClusterNode(_))

	/**
	 * Returns an array of all rcluster nodes that respond to a simple command in less than 5 seconds
	 */
	def activeNodes() = {
		val check = nodes.map((n) => (n, Future(n.executeCommand("hostname"))))
		Thread.sleep(5000)
		check.filter((c) => c._2.isDone && c._2.success).map((c) => c._1)
	}
}

/**
 * Concrete child of a RemoteMachine customized for working with the RCluster
 */
class RClusterNode(num: Int) extends RemoteMachine with RunitManager {
	val hostname = "r" + num + ".millennium.berkeley.edu"
	val username = "marmbrus"
	val rootDirectory = new File("/scratch/marmbrus/")
	val privateKey = new File("/Users/marmbrus/.ssh/id_rsa")
	val runitBinaryPath = new File("/work/marmbrus/runit")

	def setupRunit() {
		executeCommand("mkdir -p " + serviceRoot) match {
			case ExecuteResponse(Some(0), "", "") => logger.debug("Service Directory Created")
			case e: ExecuteResponse => logger.fatal("Unexpected execution result while creating service directory: " + e)
		}

		val runsvdirCommand = new Regex("runsvdir " + serviceRoot)

		executeCommand("ps ax | grep runsvdir") match {
			case ExecuteResponse(Some(0), whatsRunning, "") => {
				runsvdirCommand.findFirstIn(whatsRunning) match {
					case Some(_) => logger.debug("runsvdir already running on " + this)
					case None => {
						logger.debug("Starting runsv since we only found " + whatsRunning)
						logger.debug(executeCommand("/bin/bash -c \"PATH=" + runitBinaryPath + " /usr/bin/nohup " + runsvdirCmd + " " + serviceRoot + "\"&"))
					}
				}
			}
			case e: ExecuteResponse => logger.fatal("Unexpected execution result while checking for runsvdir: " + e)
		}
	}
}
