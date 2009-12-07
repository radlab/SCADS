package deploylib.chef

import org.json.JSONObject
import org.json.JSONArray
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
  
  override def getJSONConfig: String = {
    val nginxConfig = new JSONObject()
    val nginxRecipes = new JSONArray()
    nginxRecipes.put("cloudstone::nginx")
    nginxRecipes.put("cloudstone::faban-agent")
    nginxConfig.put("recipes", nginxRecipes)
    val nginxNginx = new JSONObject()
    val nginxNginxServers = new JSONObject()
    
    haproxy.foreach(instance => {
      val server = new JSONObject()
      server.put("start", 4000)
      server.put("count", 1)
      nginxNginxServers.put(instance.privateDnsName, server)
    })
    nginxNginx.put("servers", nginxNginxServers)
    nginxConfig.put("nginx", nginxNginx)
    
    val nginxFaban = new JSONObject()
    nginxFaban.put("mysql", false)
    nginxFaban.put("postgresql", false)
    nginxConfig.put("faban", nginxFaban)
  }
  
}
