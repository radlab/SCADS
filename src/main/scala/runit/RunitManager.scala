package deploylib.runit

import deploylib._
import java.io.File

/**
 * Allows the management of services controlled by Runit on the remote machine
 */
abstract trait RunitManager extends RemoteMachine {
	def services: List[RunitService] = {
		executeCommand("cd " + serviceRoot + ";ls") match {
      case ExecuteResponse(Some(0), "", "") => List[RunitService]()
			case ExecuteResponse(Some(0), services, "") => services.split("\n").toList.map(new RunitService(this, _))
			case e: ExecuteResponse => {
				logger.fatal("Unexpected ExecuteResponse while listing services: " + e)
				List[RunitService]()
			}
		}
	}

	def clearAll: Unit = {
		services.foreach(s => {
				s.stop
				logger.debug("deleting " + new File(s.serviceDir, "*"))
				executeCommand("rm -rf " + s.serviceDir)
		})
	}
}
