package deploylib.chef

import deploylib._

/*************************
{
    "recipes": ["cloudstone::nginx"],
    "nginx": {
        "servers": {
            "localhost": {
                "start": 3000,
                "count": 2
            }
        }
    }
}
*************************/

case class NginxService(remoteMachine: RemoteMachine,
                        config: Map[String,Any]) extends ChefService(remoteMachine, config) {
  val cookbookName = "cloudstone"
  val recipeName = "rails"

  remoteMachine.addService(this)

  /**
   * The name of the chef recipe used to deploy this service.
   */
  val recipeName: String
  
}
