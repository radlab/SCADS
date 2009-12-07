package deploylib.chef

import deploylib._

case class FabanAgentService(remoteMachine: RemoteMachine,
                             config: Map[String,Any]) extends ChefService(remoteMachine, config) {
  val cookbookName = "cloudstone"
  val recipeName = "rails"

  remoteMachine.addService(this)

}
