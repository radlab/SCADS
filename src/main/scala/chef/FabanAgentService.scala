package deploylib.chef

import org.json.JSONObject
import org.json.JSONArray
import deploylib._

/*************************
{
    "faban": {
        "jdbc": "mysql"
    },
    "recipes": ["cloudstone::faban-agent"]
}
*************************/

case class FabanAgentService(remoteMachine: RemoteMachine,
                             config: Map[String,Any]) extends ChefService(remoteMachine, config) {
  val cookbookName = "cloudstone"
  val recipeName = "faban-agent"

  remoteMachine.addService(this)

}
