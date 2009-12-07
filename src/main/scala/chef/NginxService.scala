package deploylib.chef

import deploylib._

/*************************
{
    "recipes": ["cloudstone::nginx", "cloudstone::faban-agent"],
    "nginx": {
        "servers": {
            "localhost": {
                "start": 3000,
                "count": 2
            }
        }
    },
    "faban": {
        "jdbc": null
    }
}
*************************/

case class NginxService(remoteMachine: RemoteMachine,
                        config: Map[String,Any]) extends ChefService(remoteMachine, config) {
  val cookbookName = "cloudstone"
  val recipeName = "nginx"

  remoteMachine.addService(this)

  /**
   * Service-specific variables.
   */
  var haproxyService: HAProxyService = null
  var railsServices: Set[RailsService] = new Set[RailsService]()

  /**
   * Update the JSON config object and add to dependencies.
   */
  override def addDependency(service: Service): Unit = {
    service match {
      case HAProxyService(_) =>
        haproxyService = service.asInstanceOf[HAProxyService]
      case RailsService(_) =>
        railsServices += service.asInstanceOf[RailsService]
      case _ =>
        super(service)
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
