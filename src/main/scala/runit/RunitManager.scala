package deploylib.runit

import deploylib.ServiceManager

import java.io.File

/**
 * Allows the management of services controlled by Runit on the remote machine
 */
abstract trait RunitManager extends ServiceManager {
	val runitBinaryPath: File
	lazy val runsvdirCmd: File = new File(runitBinaryPath, "runsvdir")
	lazy val svCmd: File = new File(runitBinaryPath, "sv")
	lazy val svlogdCmd: File = new File(runitBinaryPath, "svlogd")
	lazy val serviceRoot = new File(rootDirectory, "services")

	def services: Array[Service] = {
		executeCommand("find " + serviceRoot) match {
			case ExecuteResponse(Some(0), services, "") => services.split("\n").map(new RunitService(this, _))
			case e: ExecuteResponse => {
				logger.fatal("Unexpected ExecuteResponse while listing services: " + e)
				Array[Service]()
			}
		}	
	}
	
	def clearAll: Unit = null
}
