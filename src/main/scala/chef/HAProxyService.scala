package deploylib.chef

import org.json.JSONObject
import org.json.JSONArray
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
  var railsServices: Set[RailsService] = Set()

  /**
   * Update the JSON config object and add to dependencies.
   */
  override def addDependency(service: Service): Unit = {
    service match {
      case RailsService(_,_) =>
        railsServices += service.asInstanceOf[RailsService]
      case _ =>
        super.addDependency(service)
    }
  }

  override def start: Unit = {
    // TODO: Upload JSON Config
    // TODO: Execute command to run recipe
  }

  override def getJSONConfig: String = {
    val haproxyConfig = new JSONObject()
    haproxyConfig.put("recipes", new JSONArray().put("cloudstone::haproxy"))
    val haproxyHaproxy = new JSONObject()
    
    val haproxyHaproxyServers = new JSONObject()
    if (railsServices.isEmpty)
    // TODO: Throw exception 
    for (service <- railsServices) {
      val server = new JSONObject()
      server.put("start", service.startPort)
      server.put("count", service.count)
      haproxyHaproxyServers.put(instance.privateDnsName, server)
    }
    haproxyHaproxy.put("servers", haproxyHaproxyServers)
    
    haproxyConfig.put("haproxy", haproxyHaproxy)
    
    return haproxyConfig.toString
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
