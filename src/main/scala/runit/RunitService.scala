package deploylib.runit

import deploylib.Service

case class RunitService(manager: RunitManager, name: String) extends Service {
	def start: Unit = null
	def stop: Unit = null
	def once: Unit = null

	
	def tailLog: String = null
	def watchLog: Unit = null
}
