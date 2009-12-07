package deploylib.chef

import org.json.JSONObject
import org.json.JSONArray
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
  var fabanService: FabanService = null

  var logLevel = "error"
  if (config.contains("log_level")) {
    logLevel = config("log_level").asInstanceOf[String]
  }

  var portStart = 3000
  var portCount = 16
  if (config.contains("ports")) {
    var portsConfig = config("ports").asInstanceOf[Map[String,Any]]
    if (portsConfig.contains("start")) {
      portStart = portsConfig("start").asInstanceOf[Int]
    }
    if (portsConfig.contains("count")) {
      portCount = portsConfig("count").asInstanceOf[Int]
    }
  }

  /**
   * Update the JSON config object and add to dependencies.
   */
  override def addDependency(service: Service): Unit = {
    service match {
      case HAProxyService(_,_) =>
        haproxyService = service.asInstanceOf[HAProxyService]
      case NginxService(_,_) =>
        nginxService = service.asInstanceOf[NginxService]
      case MySQLService(_,_) =>
        mysqlService = service.asInstanceOf[MySQLService]
      case FabanService(_,_) =>
        fabanService = service.asInstanceOf[FabanService]
        // TODO: Update jsonConfig.
      case _ =>
        super.addDependency(service)
    }
  }

  override def start: Unit = {
    if (mysqlService == null) {
      // TODO: Throw an exception for missing dependency.
    }
    
    // TODO: Upload JSON Config
    // TODO: Execute command to run recipe
    
    print "Rails deployed!"
    
    // TODO: Add this to HAProxy's config, then restart it.
    if (haproxyService != null) {
      haproxyService.addRails(this)
    }
  }

  override def stop: Unit = {
    // TODO: Implement me.
  }

  override def getJSONConfig: String = {
    val railsConfig = new JSONObject()
    railsConfig.put("recipes", new JSONArray().put("cloudstone::rails"))
    val railsRails = new JSONObject()
    
    val railsRailsPorts = new JSONObject()
    railsRailsPorts.put("start", portStart)
    railsRailsPorts.put("count", portCount)
    railsRails.put("ports", railsRailsPorts)
    
    val railsRailsDatabase = new JSONObject()
    railsRailsDatabase.put("host", mysqlService.remoteMachine.hostname)
    railsRailsDatabase.put("adapter", "mysql")
    railsRailsDatabase.put("port", mysqlService.port)
    railsRails.put("database", railsRailsDatabase)
    
    val railsRailsMemcached = new JSONObject()
    railsRailsMemcached.put("host", "localhost")
    railsRailsMemcached.put("port", 1211)
    railsRails.put("memcached", railsRailsMemcached)
    
    val railsRailsGeocoder = new JSONObject()
    if (fabanService != null)
      railsRailsGeocoder.put("host", fabanService.remoteMachine.hostname)
    else
      railsRailsGeocoder.put("host", "localhost")
    railsRailsGeocoder.put("port", 9980)
    railsRails.put("geocoder", railsRailsGeocoder)
    
    railsConfig.put("rails", railsRails)
    
    return railsConfig.toString
  }

}
