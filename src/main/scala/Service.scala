package deploylib

/**
 * Provides a framework for deploying and managing services.
 */
abstract class Service(remoteMachine: RemoteMachine) {

  /**
   * Dependency management because services can depend on other services.
   */
  def addDependency(service: Service): Unit

  /**
   * Deploys and starts the service.
   */
  def start: Unit

  /**
   * Shuts down the service.
   */
  def stop: Unit

}
