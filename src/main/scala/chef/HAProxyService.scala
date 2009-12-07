package deploylib.chef

import deploylib._

/*************************
{
    "recipes": ["cloudstone::haproxy"],
    "haproxy": {
        "servers": {
            "localhost": {
                "start": 3000,
                "count": 2
            }
        },
        "metric_service": {
            "host": null,
            "port": null
        }
    }
}
*************************/

case class HAProxyService(remoteMachine: RemoteMachine,
                          config: Map[String,Any]) extends ChefService(remoteMachine, config) {
  val cookbookName = "cloudstone"
  val recipeName = "haproxy"

  remoteMachine.addService(this)

  /**
   * Service-specific variables.
   */
  var railsServices: Set[RailsService] = new Set[RailsService]()

  /**
   * Update the JSON config object and add to dependencies.
   */
  override def addDependency(service: Service): Unit = {
    service match {
      case RailsService(_) =>
        railsServices += service
      case _ =>
        // TODO: Throw an exception for unhandled dependency.
    }
  }

  override def start: Unit = {
    // TODO: Upload JSON Config
    // TODO: Execute command to run recipe
  }

  /**
   * Service-specific methods.
   */

  /**
   * Adds a Rails machine to the configuration of HAProxy and restarts it.
   */
  def addRails: Unit = {
    // TODO: Implement me.
  }

}
