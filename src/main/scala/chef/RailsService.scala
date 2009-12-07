package deploylib.chef

import deploylib._

/*************************
{
    "rails": {
        "log_level": "debug",
        "geocoder": {
            "port": 9980,
            "host": "localhost"
        },
        "memcached": {
            "port": 1211,
            "host": "localhost"
        },
        "database": {
            "port": 3306,
            "adapter": "mysql",
            "host": "domU-12-31-39-03-14-71.compute-1.internal"
        },
        "ports": {
            "count": 16,
            "start": 3000
        }
    },
    "recipes": ["cloudstone::rails"]
}
*************************/

case class RailsService(remoteMachine: RemoteMachine,
                        config: Map[String,Any]) extends ChefService(remoteMachine, config) {
  val cookbookName = "cloudstone"
  val recipeName = "rails"

  remoteMachine.addService(this)

  /**
   * Service-specific variables.
   */
  var haproxyService: HAProxyService = null
  var nginxService: NginxService = null
  var mysqlService: MySQLService = null

  /**
   * Update the JSON config object and add to dependencies.
   */
  override def addDependency(service: Service): Unit = {
    service match {
      case HAProxyService(_, _) =>
        haproxyService = service
      case NginxService(_, _) =>
        nginxService = service
      case MySQLService(_, _) =>
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
  
  override def getJSONConfig: String = {
    
  }

}
