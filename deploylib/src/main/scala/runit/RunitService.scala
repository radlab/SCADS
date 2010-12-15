package deploylib.runit

import deploylib._
import java.io.File
import scala.util.matching.Regex
import net.lag.logging.Logger

case class RunitStatus(status: String, pid: Int, upTime: Int)

object BadStatus extends Exception
case class UnsucessfulSvCmdException(resp: ExecuteResponse) extends Exception

case class RunitService(manager: RunitManager, name: String) {
	val logger = Logger()
	val serviceDir = new File(manager.serviceRoot, name)
	val superviseDir = new File(serviceDir, "supervise")
	val statFile = new File(superviseDir, "stat")
	val controlFile = new File(superviseDir, "control")
	val logDir = new File(serviceDir, "log")
	val logFile = new File(logDir, "current")
	val failureFile = new File(serviceDir, "failures")

	def svCmd(cmd: String): Unit = {
		manager.executeCommand(manager.svCmd + " " + cmd + " " + serviceDir) match {
			case ExecuteResponse(Some(0), out, "") => logger.debug("Service " + name + " on " + manager.hostname + " " + cmd)
			case e => {
				logger.warning("Unexpected result while running sv on " + manager.hostname + ": " + e)
				throw new UnsucessfulSvCmdException(e)
			}
		}
	}

	def once: Unit = svCmd("once")
	def start: Unit = svCmd("up")
	def stop: Unit = svCmd("down")
	def exit: Unit = svCmd("exit")

	val downRegex = new Regex("""ok: down: \S+ (\d+)s.*""" + "\n")
	val runRegex = new Regex("""ok: (\S+): \S+ \(pid (\d+)\) (\d+)s.*""" + "\n")

	def status: RunitStatus = manager.executeCommand(manager.svCmd + " check " + serviceDir) match {
		case ExecuteResponse(Some(0), "", "") => RunitStatus("down", -1, 0)
		case ExecuteResponse(Some(0), downRegex(sec), "") => RunitStatus("down", -1, sec.toInt)
		case ExecuteResponse(Some(0), runRegex(status, pid, sec), "") => RunitStatus(status, pid.toInt, sec.toInt)
		case ExecuteResponse(Some(1), _, _) => RunitStatus("nonexistant", -1, 0)
		case unknown => {
			logger.warning("Unable to parse service status: " + unknown)
			throw BadStatus
		}
	}

	def blockTillUpFor(seconds: Int):Unit = {
		var curStatus = status
		while(curStatus.upTime < seconds && (!curStatus.status.equals("run"))) {
			logger.info("Waiting for " + name + " on " + manager.hostname)
			start
			Thread.sleep(1000)
			curStatus = status
		}
	}

  def blockTillDown: Unit = {
    while(!status.status.equals("down")) {
      logger.info(name + " still running on " + manager.hostname + ", sleeping for 10 seconds")
      Thread.sleep(10000)
    }
  }

	def clearFailures: Unit = manager.executeCommand("rm -f " + failureFile)
	def clearLog: Unit = manager.executeCommand("rm -f " + logFile)

	def tailLog: String = manager.tail(logFile)
	def watchLog: Unit = manager.watch(logFile)
	def watchFailures: Unit = manager.watch(failureFile)
}
