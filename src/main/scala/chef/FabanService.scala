package deploylib.chef

import org.json.JSONObject
import org.json.JSONArray
import deploylib._
import java.io.File

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
  var nginxService: NginxService = null

  var debug = false
  if (config.contains("debug")) {
    debug = config("debug").asInstanceOf[Boolean]
  }

  /**
   * Update the JSON config object and add to dependencies.
   */
  override def addDependency(service: Service): Unit = {
    service match {
      case MySQLService(_,_) =>
        mysqlService = service.asInstanceOf[MySQLService]
      case NginxService(_,_) =>
        nginxService = service.asInstanceOf[NginxService]
      case _ =>
        super.addDependency(service)
    }
  }

  override def start: Unit = {
    // TODO: Upload JSON Config
    // TODO: Execute command to run recipe
    super.start
    remoteMachine.createFile(new File("fabanConfig"), getJSONConfig)
    remoteMachine.executeCommand("chef-solo -r /tmp/repo.tar.gz -j fabanConfig")
    println("Faban Deployed!")
  }

  override def stop: Unit = {
    // TODO: Implement me.
  }

  override def getJSONConfig: String = {
    val fabanConfig = new JSONObject()
    fabanConfig.put("recipes", new JSONArray().put("cloudstone::faban"))
    val fabanFaban = new JSONObject()
    val fabanFabanHosts = new JSONObject()
    fabanFabanHosts.put("driver", remoteMachine.hostname)
    fabanFabanHosts.put("webserver", nginxService.remoteMachine.hostname)
    fabanFabanHosts.put("database", mysqlService.remoteMachine.hostname)
    fabanFabanHosts.put("storage", "")
    fabanFabanHosts.put("cache", "")

    val fabanFabanDatabase = new JSONObject()
    fabanFabanDatabase.put("adapter", "mysql")
    fabanFabanDatabase.put("port", mysqlService.port)

    fabanFaban.put("hosts", fabanFabanHosts)
    fabanFaban.put("database", fabanFabanDatabase)
    fabanFaban.put("debug", debug)
    fabanConfig.put("faban", fabanFaban)

    return fabanConfig.toString
  }

}
