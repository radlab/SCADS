package deploylib.chef

import deploylib._

/*************************
{
    "recipes": ["cloudstone::nginx"],
    "nginx": {
        "servers": {
            "localhost": {
                "start": 3000,
                "count": 2
            }
        }
    }
}
*************************/

case class NginxService(remoteMachine: RemoteMachine,
                        config: Map[String,Any]) extends ChefService(remoteMachine, config) {
  val cookbookName = "cloudstone"
  val recipeName = "rails"

  remoteMachine.addService(this)

  /**
   * Service-specific variables.
   */
  var haproxyService: HAProxyService = null
  var railsServices: Set[RailsService] = null

  /**
   * Update the JSON config object and add to dependencies.
   */
  override def addDependency(service: Service): Unit = {
    service match {
      case HAProxyService(_) =>
        haproxyService = service
      case RailsService(_) =>
        railsServices += service
      case _ =>
        // TODO: Throw an exception for unhandled dependency.
    }
  }

  override def start: Unit = {
    // TODO: Upload JSON Config
    if (haproxyService == null) {
      // Use rails services.
    } else {
      // Use haproxy service.
    }
    // TODO: Execute command to run recipe
  }
  
}
