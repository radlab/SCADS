package deploylib.chef

import org.json.JSONObject
import org.json.JSONArray
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
  var railsServices: Set[RailsService] = Set()

  /**
   * Update the JSON config object and add to dependencies.
   */
  override def addDependency(service: Service): Unit = {
    service match {
      case HAProxyService(_,_) =>
        haproxyService = service.asInstanceOf[HAProxyService]
      case RailsService(_,_) =>
        railsServices += service.asInstanceOf[RailsService]
      case _ =>
        super.addDependency(service)
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

  override def stop: Unit = {
    // TODO: Implement me.
  }

  override def getJSONConfig: String = {
    val nginxConfig = new JSONObject()
    val nginxRecipes = new JSONArray()
    nginxRecipes.put("cloudstone::nginx")
    nginxRecipes.put("cloudstone::faban-agent")
    nginxConfig.put("recipes", nginxRecipes)
    val nginxNginx = new JSONObject()
    val nginxNginxServers = new JSONObject()
    
    if (haproxyService != null) {
      val server = new JSONObject()
      server.put("start", haproxyService.port)
      server.put("count", 1)
      nginxNginxServers.put(haproxyService.remoteMachine.hostname, server)
    } else if (!railsServices.isEmpty) {
      for (rs <- railsServices) {
        val server = new JSONObject
        server.put("start", rs.portStart)
        server.put("count", rs.portCount)
        nginxNginxServers.put(rs.remoteMachine.hostname, server)
      }
    } else {
      // TODO: throw exception, no haproxy or rails services
    }

    nginxNginx.put("servers", nginxNginxServers)
    nginxConfig.put("nginx", nginxNginx)
    
    val nginxFaban = new JSONObject()
    nginxFaban.put("jdbc", null: String)
    nginxConfig.put("faban", nginxFaban)
    
    return nginxConfig.toString
  }
  
}
