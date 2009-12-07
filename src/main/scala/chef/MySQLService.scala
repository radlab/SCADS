package deploylib.chef

import org.json.JSONObject
import org.json.JSONArray
import deploylib._

/*************************
{
    "recipes": ["cloudstone::mysql", "cloudstone::faban-agent"],
    "mysql": {
        "port": 3306,
        "server_id": 1
    },
        "faban": {
        "jdbc": "mysql"
    }
}
*************************/

case class MySQLService(remoteMachine: RemoteMachine,
                        config: Map[String,Any]) extends ChefService(remoteMachine, config) {
  val cookbookName = "cloudstone"
  val recipeName = "mysql"

  remoteMachine.addService(this)

  /**
   * Service-specific variables.
   */
  var port = 3306
  if (config.contains("port")) {
    port = config("port").asInstanceOf[Int]
  }

  override def start: Unit = {
    // TODO: Upload JSON Config
    // TODO: Execute command to run recipe
    print "MySQL deployed!"
  }

  override def stop: Unit = {
    // TODO: Implement me.
  }

  override def getJSONConfig: String = {
    val mysqlConfig = new JSONObject()
    val mysqlRecipes = new JSONArray()
    mysqlRecipes.put("cloudstone::mysql")
    mysqlRecipes.put("cloudstone::faban-agent")
    mysqlConfig.put("recipes", mysqlRecipes)
    val mysqlMysql = new JSONObject()
    
    mysqlMysql.put("server_id", 1)
    mysqlMysql.put("port", port)
    mysqlConfig.put("mysql", mysqlMysql)
    
    val mysqlFabanAgent = new JSONObject()
    mysqlFabanAgent.put("jdbc", "mysql")
    mysqlConfig.put("faban", mysqlFabanAgent)
    
    return mysqlConfig.toString
  }

}
