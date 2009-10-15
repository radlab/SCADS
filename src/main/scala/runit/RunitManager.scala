package deploylib.runit

import deploylib.ServiceManager

import java.io.File

/**
 * Allows the management of services controlled by Runit on the remote machine
 */
abstract trait RunitManager extends ServiceManager {
	def services: Array[Service] = {
		executeCommand("cd " + serviceRoot + ";ls") match {
			case ExecuteResponse(Some(0), services, "") => services.split("\n").map(new RunitService(this, _))
			case e: ExecuteResponse => {
				logger.fatal("Unexpected ExecuteResponse while listing services: " + e)
				Array[Service]()
			}
		}
	}

	def clearAll: Unit = null
}
