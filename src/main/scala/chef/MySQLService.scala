package deploylib.chef

import deploylib._

/*************************
{
    "mysql": {
        "port": 3306,
        "server_id": 1
    },
    "recipes": ["cloudstone::mysql"]
}
*************************/

case class MySQLService(remoteMachine: RemoteMachine,
                        config: Map[String,Any]) extends ChefService(remoteMachine, config) {
  val cookbookName = "cloudstone"
  val recipeName = "rails"

  remoteMachine.addService(this)

}
