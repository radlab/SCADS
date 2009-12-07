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
        mysqlService = service.asInstanceOf[MySQLService]
      case HAProxyService(_) =>
        haproxyService = service.asInstanceOf[HAProxyService]
      case NginxService(_) =>
        nginxService = service.asInstanceOf[NginxService]
      case RailsService(_) =>
        railsService = service.asInstanceOf[RailsService]
      case _ =>
        super(service)
    }
  }

  override def start: Unit = {
    // TODO: Upload JSON Config
    // TODO: Execute command to run recipe
  }

  override def getJSONConfig: String = {
    val fabanConfig = new JSONObject()
    fabanConfig.put("recipes", new JSONArray().put("cloudstone::faban"))
    val fabanFaban = new JSONObject()
    val fabanFabanHosts = new JSONObject()
    fabanFabanHosts.put("driver", faban.getFirst().privateDnsName)
    fabanFabanHosts.put("webserver", nginx.getFirst().privateDnsName)
    fabanFabanHosts.put("database", mysql.getFirst().privateDnsName)
    fabanFabanHosts.put("storage", "")
    fabanFabanHosts.put("cache", "")
    
    val fabanFabanDatabase = new JSONObject()
    fabanFabanDatabase.put("adapter", "mysql")
    fabanFabanDatabase.put("port", mysqlPort)
    
    fabanFaban.put("hosts", fabanFabanHosts)
    fabanFaban.put("database", fabanFabanDatabase)
    fabanFaban.put("debug", false)
    fabanConfig.put("faban", fabanFaban)
    
    return fabanConfig.toString
  }

}
