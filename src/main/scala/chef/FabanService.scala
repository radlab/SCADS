package deploylib.chef

import org.json.JSONObject
import org.json.JSONArray
import deploylib._

/*************************
{
    "recipes": ["cloudstone::faban"],
    "faban": {
        "hosts": {
            "driver":    "[Internal IP of Driver]",
            "webserver": "[Internal IP of Web Server]",
            "database":  "[Internal IP of DB]",
            "storage":   "",
            "cache":     ""
        },
        "database": {
            "adapter": "mysql",
            "port": 3306
        },
        "debug": false
    }
}

*************************/

case class FabanService(remoteMachine: RemoteMachine,
                        config: Map[String,Any]) extends ChefService(remoteMachine, config) {
  val cookbookName = "cloudstone"
  val recipeName = "faban"

  remoteMachine.addService(this)

  /**
   * Service-specific variables.
   */
  var mysqlService: MySQLService = null
  var haproxyService: HAProxyService = null
  var nginxService: NginxService = null
  var railsService: RailsService = null

  /**
   * Update the JSON config object and add to dependencies.
   */
  override def addDependency(service: Service): Unit = {
    service match {
      case MySQLService(_) =>
        mysqlService = service
      case HAProxyService(_) =>
        haproxyService = service
      case NginxService(_) =>
        nginxService = service
      case RailsService(_) =>
        railsService = service
      case _ =>
        // TODO: Throw an exception for unhandled dependency.
    }
  }

  override def start: Unit = {
    // TODO: Upload JSON Config
    // TODO: Execute command to run recipe
  }
}
