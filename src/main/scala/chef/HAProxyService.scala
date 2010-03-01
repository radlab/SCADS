package deploylib.chef

import org.json.JSONObject
import org.json.JSONArray
import deploylib._
import java.io.File

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

  var port = 4000 // Hardcoded.

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
    super.start
    remoteMachine.createFile(new File("haproxyConfig"), getJSONConfig)
    remoteMachine.executeCommand("chef-solo -r /tmp/repo.tar.gz -j haproxyConfig")
    println("HAProxy deployed!")
  }

  override def stop: Unit = {
    // TODO: Implement me.
  }

  override def getJSONConfig: String = {
    val haproxyConfig = new JSONObject()
    haproxyConfig.put("recipes", new JSONArray().put("cloudstone::haproxy"))
    val haproxyHaproxy = new JSONObject()

    val haproxyHaproxyServers = new JSONObject()
    if (railsServices.isEmpty) {
      // TODO: Throw exception
    }
    for (service <- railsServices) {
      print("Rails 1")
      val server = new JSONObject()
      server.put("start", service.portStart)
      server.put("count", service.portCount)
      haproxyHaproxyServers.put(service.remoteMachine.hostname, server)
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
  def addRails(railsService: RailsService): Unit = {
    // TODO: Implement me.
  }

}
