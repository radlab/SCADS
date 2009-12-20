package deploylib.rcluster

import deploylib._
import deploylib.runit._

import java.io.File
import scala.util.matching.Regex
import ch.ethz.ssh2.{Connection, Session, ChannelCondition, SCPClient}


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
		Thread.sleep(20000)
		check.foreach((c) => if(!c._2.isDone) c._2.cancel)
		check.filter((c) => c._2.isDone && c._2.success).map((c) => c._1)
	}
}

/**
 * Concrete child of a RemoteMachine customized for working with the RCluster
 */
class RClusterNode(num: Int) extends RemoteMachine with RunitManager {
	val hostname = "r" + num + ".millennium.berkeley.edu"
	val username = Util.username
	val rootDirectory = new File("/scratch/" + Util.username + "/")
	val privateKey = new File(System.getProperty("user.home"), ".ssh/id_rsa")
	val runitBinaryPath = new File("/work/" + Util.username + "/runit")
	val javaCmd = new File("/usr/lib/jvm/java-6-sun/bin/java")

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

	override def upload(localFile: File, remoteDirectory: File): Unit = {
		val cacheFile = new File("/work/marmbrus/cache", localFile.getName)
		val remoteFile = new File(remoteDirectory, localFile.getName)
		val md5Cache = Util.md5(localFile)

		if(md5Cache == md5(cacheFile))
			logger.debug("Not uploading " + localFile + " as the hashes match")
		else
			useConnection((c) => {
				val scp = new SCPClient(c)
        logger.info("Hashes don't match, uploading " + localFile + " to " + remoteDirectory)
				scp.put(localFile.toString, "/work/marmbrus/cache")
        logger.debug("Upload of " + localFile + " complete")
			})

		if(md5Cache == md5(remoteFile))
			logger.debug("Not copying " + localFile + " from cache as it matches")
		else {
			executeCommand("cp " + cacheFile + " " + remoteFile) match {
				case ExecuteResponse(Some(0), _, _) => logger.debug("Success")
				case _ => logger.warn("Copy error")
			}
		}
	}
}
