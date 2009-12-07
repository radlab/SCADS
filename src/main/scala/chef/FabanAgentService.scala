package deploylib.chef

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
