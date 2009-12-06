package deploylib.chef

import deploylib._

case class RailsService(remoteMachine: RemoteMachine,
                   jsonConfig: JSONConfig) extends ChefService {
  remoteMachine.addService(this)

  val recipeName = "rails"

  /**
   * Update the JSON config object and add to dependencies.
   */
  override def addDependency(service: Service): Unit = {
    // TODO: First, check if this is a valid dependency (e.g. is HAProxy?).
    super(service)
    // TODO: Update the JSON config.
  }

  override def start: Unit = {
    // TODO: Upload JSON Config
    // TODO: Execute command to run recipe
    
    // TODO: Add this to HAProxy's config, then restart it.
  }

}
