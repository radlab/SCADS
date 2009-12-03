package deploylib.runit

import deploylib.Service
import java.io.File
import scala.util.matching.Regex
import org.apache.log4j.Logger

case class RunitStatus(status: String, pid: Int, upTime: Int)

object BadStatus extends Exception

case class RunitService(manager: RunitManager, name: String) {
	val logger = Logger.getLogger("deploylib.runitservice")
	val serviceDir = new File(manager.serviceRoot, name)
	val superviseDir = new File(serviceDir, "supervise")
	val statFile = new File(superviseDir, "stat")
	val controlFile = new File(superviseDir, "control")
	val logDir = new File(serviceDir, "log")
	val logFile = new File(logDir, "current")

	val downRegex = new Regex("""ok: down: \S+ (\d+)s.*""" + "\n")
	val runRegex = new Regex("""ok: run: \S+ \(pid (\d+)\) (\d+)s.*""" + "\n")

	def status: RunitStatus = manager.executeCommand(manager.svCmd + " check " + serviceDir) match {
		case ExecuteResponse(Some(0), "", "") => RunitStatus("down", -1, 0)
		case ExecuteResponse(Some(0), downRegex(sec), "") => RunitStatus("down", -1, sec.toInt)
		case ExecuteResponse(Some(0), runRegex(pid, sec), "") => RunitStatus("run", pid.toInt, sec.toInt)
		case ExecuteResponse(Some(1), _, _) => RunitStatus("nonexistant", -1, 0)
		case unknown => {
			logger.warn("Unable to parse service status: " + unknown)
			throw BadStatus
		}
	}
	def start: Unit = manager.createFile(controlFile, "u")
	def stop: Unit = manager.executeCommand(manager.svCmd + " start " + serviceDir) match {
		case ExecuteResponse(Some(0), _, _) => logger.debug("Service " + name + " on " + manager.hostname + " started")
		case _ => logger.warn("Failed to start" + name + " on " + manager.hostname)
 	}

	def once: Unit = manager.createFile(controlFile, "o")

	def blockTillUpFor(seconds: Int):Unit = {
		var curStatus = status
		while(curStatus.upTime < seconds && (!curStatus.status.equals("run"))) {
			logger.info("Waiting for " + name + " on " + manager.hostname)
			start
			Thread.sleep(1000)
			curStatus = status
		}
	}

	def tailLog: String = manager.tail(logFile)
	def watchLog: Unit = manager.watch(logFile)
}
