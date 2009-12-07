package deploylib.chef

import deploylib._

case class RailsService(remoteMachine: RemoteMachine,
                        config: Map[String,Any]) extends ChefService(remoteMachine, config) {
  val cookbookName = "cloudstone"
  val recipeName = "rails"

  remoteMachine.addService(this)

  /**
   * Service-specific variables.
   */
  var haproxyService: HAProxyService = null
  var mysqlService: MySQLService = null

  /**
   * Update the JSON config object and add to dependencies.
   */
  override def addDependency(service: Service): Unit = {
    service match {
      case HAProxyService(_) =>
        haproxyService = service
        // TODO: Update jsonConfig.
      case MySQLService(_) =>
        mysqlService = service
        // TODO: Update jsonConfig.
      case _ =>
        // TODO: Throw an exception for unhandled dependency.
    }
  }

  override def start: Unit = {
    if (mysqlService == null) {
      // TODO: Throw an exception for missing dependency.
    }
    
    // TODO: Upload JSON Config
    // TODO: Execute command to run recipe
    
    // TODO: Add this to HAProxy's config, then restart it.
    if (haproxyService != null) {
      haproxyService.addRails(this)
    }
  }

}
