package deploylib

/**
 * Provides a framework for deploying and managing services.
 */
abstract class Service(remoteMachine: RemoteMachine) {

  /**
   * Dependency management because services can depend on other services.
   */
  protected var dependencies: Set[Service]
  def addDependency(service: Service): Unit = {
    dependencies += service
  }

  /**
   * Deploys and starts the service.
   */
  def start: Unit

  /**
   * Shuts down the service.
   */
  def stop: Unit

}
