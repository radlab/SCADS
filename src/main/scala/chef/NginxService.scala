package deploylib.chef

import deploylib._

/*************************
{
    "nginx": {
        "servers": {
            "domU-12-31-39-03-14-71.compute-1.internal": {
                "count": 16,
                "start": 3000
            }
        }
    },
    "recipes": ["cloudstone::nginx"]
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
