package deploylib.runit

import deploylib.ServiceManager

import java.io.File

/**
 * Allows the management of services controlled by Runit on the remote machine
 */
abstract trait RunitManager extends ServiceManager {
	def services: List[Service] = {
		executeCommand("cd " + serviceRoot + ";ls") match {
      case ExecuteResponse(Some(0), "", "") => List[Service]()
			case ExecuteResponse(Some(0), services, "") => services.split("\n").toList.map(new RunitService(this, _))
			case e: ExecuteResponse => {
				logger.fatal("Unexpected ExecuteResponse while listing services: " + e)
				List[Service]()
			}
		}
	}

	def clearAll: Unit = null
}
