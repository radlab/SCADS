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

  override def start: Unit = {
    // TODO: Upload JSON Config
    // TODO: Execute command to run recipe
  }

}
