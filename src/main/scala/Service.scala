package deploylib

/**
 * Alows a user to control and monitor a specific running service on a remote machine.
 * Services are returned by a RemoteMachine that implements a concrete ServiceManager
 */
abstract class Service {
	val name: String

	/**
	 * Start a service
	 */
	def start: Unit

	/**
	 * Stop a running service
	 */
	def stop: Unit

	/**
	 * Start a service, but don't restart it if it fails
	 */
	def once: Unit


	/**
   * Return the last 20 lines from the logfile for this service
   */
	def tailLog: String

	/**
   * Start a background thread that will ouput any new log messages from this service to STDOUT
   */
	def watchLog: Unit
}

/**
 * Allows the control of a set of services
 */
abstract trait ServiceManager extends RemoteMachine {
	def services: Array[Service]
	def stopAll: Unit = services.foreach(_.stop)
	def clearAll: Unit
}
