package deploylib.runit

import deploylib.Service
import java.io.File

case class RunitService(manager: RunitManager, name: String) extends Service {
	val serviceDir = new File(manager.serviceRoot, name)
	val superviseDir = new File(serviceDir, "supervise")
	val statFile = new File(superviseDir, "stat")
	val controlFile = new File(superviseDir, "control")
	val logDir = new File(serviceDir, "log")
	val logFile = new File(logDir, "current")

	def status: String = manager.catFile(statFile)
	def start: Unit = manager.createFile(controlFile, "u")
	def stop: Unit = manager.createFile(controlFile, "d")
	def once: Unit = manager.createFile(controlFile, "o")

	def tailLog: String = manager.tail(logFile)
	def watchLog: Unit = manager.watch(logFile)
}
