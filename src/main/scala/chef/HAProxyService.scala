package deploylib.chef

import deploylib._

/*************************
{
    "recipes": ["cloudstone::haproxy"],
    "haproxy": {
        "servers": {
            "localhost": {
                "start": 3000,
                "count": 2
            }
        },
        "metric_service": {
            "host": null,
            "port": null
        }
    }
}
*************************/

case class HAProxyService(remoteMachine: RemoteMachine,
                          config: Map[String,Any]) extends ChefService(remoteMachine, config) {
  val cookbookName = "cloudstone"
  val recipeName = "rails"

  remoteMachine.addService(this)

  def addRails: Unit = {
    // TODO: Implement me.
  }

}
